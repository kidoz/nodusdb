use std::collections::HashMap;
use std::io::Error as IoError;
use std::sync::Arc;
use std::sync::RwLock;

use bytes::Buf;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tracing::{error, info};

use nodus_catalog::PrincipalId;
use nodus_security::{PasswordAuthenticator, Session, SessionRegistry};
use pgwire::api::auth::{DefaultServerParameterProvider, StartupHandler};
use pgwire::api::copy::CopyHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::tokio::PgWireMessageServerCodec;

mod client_meta;
mod copy;
mod encoding;
mod extended_query;
mod simple_query;
mod startup;
mod type_map;
mod wire_format;
use client_meta::*;
use copy::NodusCopyHandler;
use extended_query::NodusExtendedQueryHandler;
use startup::NodusStartupHandler;
use type_map::map_declared_type;
use wire_format::*;

pub(crate) const METADATA_NODUS_SESSION_ID: &str = "nodus_session_id";
pub(crate) const METADATA_NODUS_PRINCIPAL_ID: &str = "nodus_principal_id";
pub(crate) const METADATA_BACKEND_PID: &str = "nodus_backend_pid";
pub(crate) const METADATA_BACKEND_SECRET: &str = "nodus_backend_secret";
pub(crate) const METADATA_TX_STATUS: &str = "nodus_tx_status";
pub(crate) const METADATA_COPY_ROWS: &str = "nodus_copy_rows";
pub(crate) const METADATA_COPY_EXTENDED: &str = "nodus_copy_extended";
pub(crate) const METADATA_STATEMENT_TIMEOUT_MS: &str = "nodus_statement_timeout_ms";

pub(crate) const POSTGRES_TYPEMOD_NONE: i32 = -1;

const CANCEL_REQUEST_MAGIC: i32 = 80877102;
const SSL_REQUEST_MAGIC: i32 = 80877103;
const GSSENC_REQUEST_MAGIC: i32 = 80877104;
const STARTUP_PACKET_HEADER_LEN: usize = 8;
const CANCEL_REQUEST_LEN: usize = 16;

/// Maps executor error text to the closest SQLSTATE so clients can react to
/// the failure class (constraint handling, missing-relation fallbacks during
/// introspection) instead of treating every error as an internal server fault.
use pgwire::api::{ClientInfo, DefaultClient, PgWireConnectionState, PgWireHandlerFactory};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::response::{ReadyForQuery, SslResponse};
use uuid::Uuid;

/// Runs the synchronous executor on the blocking pool so the calling reactor
/// worker is never parked. This is required for correctness: executor write
/// paths route through the async `RaftRouter`, which waits via `blocking_recv`
/// and would panic (and risk worker-pool starvation) if called on a runtime
/// worker thread.
pub(crate) async fn execute_off_reactor(
    executor: Arc<dyn nodus_executor::Executor>,
    ctx: nodus_executor::ExecutionContext,
    plan: nodus_executor::LogicalPlan,
) -> anyhow::Result<nodus_executor::QueryOutput> {
    match tokio::task::spawn_blocking(move || executor.execute_logical(&ctx, plan)).await {
        Ok(result) => result,
        Err(join_err) => Err(anyhow::anyhow!("execution task failed: {join_err}")),
    }
}

pub struct NodusQueryHandler {
    /// Fallback session id for tests that instantiate the handler directly.
    pub session_id: String,
    pub session_state: nodus_sql::SessionState,
    pub executor: Arc<dyn nodus_executor::Executor>,
    pub metrics: nodus_monitoring::Metrics,
    pub(crate) registry: Arc<SessionRegistry>,
    pub(crate) slow_log: Arc<nodus_monitoring::SlowQueryLog>,
}

/// Observes query latency on drop, so every `do_query` return path is covered:
/// records into the histogram and, if slow, into the slow-query log.
pub(crate) struct QueryTimer<'a> {
    pub(crate) start: std::time::Instant,
    pub(crate) sql: &'a str,
    pub(crate) session_id: &'a str,
    pub(crate) metrics: &'a nodus_monitoring::Metrics,
    pub(crate) slow_log: &'a nodus_monitoring::SlowQueryLog,
}

impl Drop for QueryTimer<'_> {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        self.metrics
            .query_latency_seconds
            .observe(elapsed.as_secs_f64());
        let ms = elapsed.as_millis() as u64;
        if ms >= self.slow_log.threshold_ms() {
            self.slow_log
                .record(self.sql.to_string(), ms, self.session_id.to_string());
            self.metrics.slow_queries_total.inc();
        }
    }
}

pub(crate) struct CurrentQueryGuard<'a> {
    pub(crate) registry: &'a SessionRegistry,
    pub(crate) session_id: &'a str,
}

impl Drop for CurrentQueryGuard<'_> {
    fn drop(&mut self) {
        self.registry.finish_current_query(self.session_id);
    }
}

impl Drop for NodusQueryHandler {
    fn drop(&mut self) {
        if !self.session_id.is_empty() {
            self.registry.deregister(&self.session_id);
        }
    }
}

pub struct NodusPgWireServer {
    startup_handler: Arc<NodusStartupHandler>,
    executor: Arc<dyn nodus_executor::Executor>,
    metrics: nodus_monitoring::Metrics,
    registry: Arc<SessionRegistry>,
    slow_log: Arc<nodus_monitoring::SlowQueryLog>,
    extended_query_handler: Arc<NodusExtendedQueryHandler>,
    copy_handler: Arc<NodusCopyHandler>,
}

impl PgWireHandlerFactory for NodusPgWireServer {
    type StartupHandler = NodusStartupHandler;
    type SimpleQueryHandler = NodusQueryHandler;
    type ExtendedQueryHandler = NodusExtendedQueryHandler;
    type CopyHandler = NodusCopyHandler;

    // Called once per connection by `process_socket`, so each client gets its
    // own session registered in the shared registry.
    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        Arc::new(NodusQueryHandler {
            session_id: String::new(),
            session_state: nodus_sql::SessionState::default(),
            executor: self.executor.clone(),
            metrics: self.metrics.clone(),
            registry: self.registry.clone(),
            slow_log: self.slow_log.clone(),
        })
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.extended_query_handler.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.startup_handler.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        self.copy_handler.clone()
    }
}

enum StartupControl {
    Plain(TcpStream),
    Secure(Box<tokio_rustls::server::TlsStream<TcpStream>>),
    Closed,
}

async fn negotiate_startup_control(
    mut socket: TcpStream,
    tls: Option<Arc<tokio_rustls::TlsAcceptor>>,
    registry: Arc<SessionRegistry>,
) -> Result<StartupControl, IoError> {
    loop {
        let mut header = [0u8; STARTUP_PACKET_HEADER_LEN];
        loop {
            let read = socket.peek(&mut header).await?;
            if read == 0 {
                return Ok(StartupControl::Closed);
            }
            if read >= STARTUP_PACKET_HEADER_LEN {
                break;
            }
        }
        let magic = (&header[4..8]).get_i32();
        match magic {
            CANCEL_REQUEST_MAGIC => {
                let mut packet = [0u8; CANCEL_REQUEST_LEN];
                socket.read_exact(&mut packet).await?;
                let mut body = &packet[8..];
                let pid = body.get_i32();
                let secret = body.get_i32();
                let accepted = registry.cancel_backend_query(pid, secret);
                info!(
                    "Received cancel request for backend pid={} accepted={}",
                    pid, accepted
                );
                return Ok(StartupControl::Closed);
            }
            SSL_REQUEST_MAGIC => {
                let mut packet = [0u8; STARTUP_PACKET_HEADER_LEN];
                socket.read_exact(&mut packet).await?;
                if let Some(acceptor) = tls {
                    socket.write_all(&[SslResponse::BYTE_ACCEPT]).await?;
                    let tls_socket = acceptor.accept(socket).await?;
                    return Ok(StartupControl::Secure(Box::new(tls_socket)));
                }
                socket.write_all(&[SslResponse::BYTE_REFUSE]).await?;
            }
            GSSENC_REQUEST_MAGIC => {
                let mut packet = [0u8; STARTUP_PACKET_HEADER_LEN];
                socket.read_exact(&mut packet).await?;
                socket.write_all(&[SslResponse::BYTE_REFUSE]).await?;
            }
            _ => return Ok(StartupControl::Plain(socket)),
        }
    }
}

fn register_socket_session<S>(
    framed: &mut Framed<S, PgWireMessageServerCodec<String>>,
    registry: &SessionRegistry,
) -> String {
    let session_id = Uuid::new_v4().to_string();
    let secret_uuid = Uuid::new_v4();
    let secret = (secret_uuid.as_u128() & 0x7fff_ffff) as i32;
    let pid = std::process::id() as i32;
    let session = Session {
        session_id: session_id.clone(),
        principal_id: PrincipalId::new(),
        active_roles: vec![],
        database_id: None,
    };
    registry.register(&session);
    registry.register_backend_key(&session_id, pid, secret);
    framed
        .codec_mut()
        .client_info
        .metadata
        .insert(METADATA_NODUS_SESSION_ID.to_owned(), session_id.clone());
    framed
        .codec_mut()
        .client_info
        .metadata
        .insert(METADATA_BACKEND_PID.to_owned(), pid.to_string());
    framed
        .codec_mut()
        .client_info
        .metadata
        .insert(METADATA_BACKEND_SECRET.to_owned(), secret.to_string());
    framed
        .codec_mut()
        .client_info
        .metadata
        .insert(METADATA_TX_STATUS.to_owned(), "I".to_owned());
    session_id
}

async fn process_nodus_message<S>(
    message: pgwire::messages::PgWireFrontendMessage,
    socket: &mut Framed<S, PgWireMessageServerCodec<String>>,
    factory: Arc<NodusPgWireServer>,
) -> PgWireResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    match socket.codec().client_info.state() {
        PgWireConnectionState::AwaitingStartup
        | PgWireConnectionState::AuthenticationInProgress => {
            factory
                .startup_handler()
                .on_startup(socket, message)
                .await?;
        }
        PgWireConnectionState::AwaitingSync => {
            if let pgwire::messages::PgWireFrontendMessage::Sync(sync) = message {
                factory
                    .extended_query_handler()
                    .on_sync(socket, sync)
                    .await?;
                socket.set_state(PgWireConnectionState::ReadyForQuery);
            }
        }
        PgWireConnectionState::CopyInProgress => match message {
            pgwire::messages::PgWireFrontendMessage::CopyData(copy_data) => {
                factory
                    .copy_handler()
                    .on_copy_data(socket, copy_data)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::CopyDone(copy_done) => {
                factory
                    .copy_handler()
                    .on_copy_done(socket, copy_done)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::CopyFail(copy_fail) => {
                factory
                    .copy_handler()
                    .on_copy_fail(socket, copy_fail)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::Sync(_) => {}
            pgwire::messages::PgWireFrontendMessage::Flush(_) => {
                socket.flush().await?;
            }
            _ => {
                return Err(user_error(
                    "ERROR",
                    "08P01",
                    "only COPY data, done, or fail messages are valid during COPY",
                ));
            }
        },
        _ => match message {
            pgwire::messages::PgWireFrontendMessage::Query(query) => {
                factory
                    .simple_query_handler()
                    .on_query(socket, query)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::Parse(parse) => {
                factory
                    .extended_query_handler()
                    .on_parse(socket, parse)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::Bind(bind) => {
                factory
                    .extended_query_handler()
                    .on_bind(socket, bind)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::Execute(execute) => {
                factory
                    .extended_query_handler()
                    .on_execute(socket, execute)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::Describe(describe) => {
                factory
                    .extended_query_handler()
                    .on_describe(socket, describe)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::Sync(sync) => {
                factory
                    .extended_query_handler()
                    .on_sync(socket, sync)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::Close(close) => {
                factory
                    .extended_query_handler()
                    .on_close(socket, close)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::Flush(_) => {
                socket.flush().await?;
            }
            pgwire::messages::PgWireFrontendMessage::Terminate(_) => {}
            pgwire::messages::PgWireFrontendMessage::CopyData(_)
            | pgwire::messages::PgWireFrontendMessage::CopyDone(_)
            | pgwire::messages::PgWireFrontendMessage::CopyFail(_) => {
                return Err(user_error(
                    "ERROR",
                    "08P01",
                    "COPY message outside COPY mode",
                ));
            }
            _ => {}
        },
    }
    Ok(())
}

async fn process_nodus_error<S>(
    socket: &mut Framed<S, PgWireMessageServerCodec<String>>,
    error: PgWireError,
    wait_for_sync: bool,
) -> Result<(), IoError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    mark_error_status(socket);
    match error {
        PgWireError::UserError(error_info) => {
            socket
                .feed(PgWireBackendMessage::ErrorResponse((*error_info).into()))
                .await?;
        }
        PgWireError::ApiError(e) => {
            let error_info = ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), e.to_string());
            socket
                .feed(PgWireBackendMessage::ErrorResponse(error_info.into()))
                .await?;
        }
        other => {
            let error_info =
                ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), other.to_string());
            socket
                .send(PgWireBackendMessage::ErrorResponse(error_info.into()))
                .await?;
            return socket.close().await;
        }
    }

    if wait_for_sync {
        socket.set_state(PgWireConnectionState::AwaitingSync);
    } else {
        socket
            .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                tx_status_from_client(socket),
            )))
            .await?;
    }
    socket.flush().await?;
    Ok(())
}

async fn process_framed_nodus_socket<S>(
    mut socket: Framed<S, PgWireMessageServerCodec<String>>,
    factory: Arc<NodusPgWireServer>,
) -> Result<(), IoError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    let session_id = register_socket_session(&mut socket, &factory.registry);
    while let Some(msg) = socket.next().await {
        let msg = match msg {
            Ok(msg) => msg,
            Err(e) => {
                process_nodus_error(&mut socket, e, false).await?;
                continue;
            }
        };
        if matches!(msg, pgwire::messages::PgWireFrontendMessage::Terminate(_)) {
            break;
        }
        let is_extended_query = msg.is_extended_query();
        if let Err(e) = process_nodus_message(msg, &mut socket, factory.clone()).await {
            process_nodus_error(&mut socket, e, is_extended_query).await?;
        }
    }
    factory.registry.deregister(&session_id);
    Ok(())
}

async fn process_nodus_socket(
    tcp_socket: TcpStream,
    tls: Option<Arc<tokio_rustls::TlsAcceptor>>,
    factory: Arc<NodusPgWireServer>,
) -> Result<(), IoError> {
    let addr = tcp_socket.peer_addr()?;
    tcp_socket.set_nodelay(true)?;

    match negotiate_startup_control(tcp_socket, tls, factory.registry.clone()).await? {
        StartupControl::Plain(socket) => {
            let client_info = DefaultClient::new(addr, false);
            let framed = Framed::new(socket, PgWireMessageServerCodec::new(client_info));
            process_framed_nodus_socket(framed, factory).await
        }
        StartupControl::Secure(socket) => {
            let client_info = DefaultClient::new(addr, true);
            let framed = Framed::new(*socket, PgWireMessageServerCodec::new(client_info));
            process_framed_nodus_socket(framed, factory).await
        }
        StartupControl::Closed => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn start_pgwire_server(
    listener: TcpListener,
    executor: Arc<dyn nodus_executor::Executor>,
    metrics: nodus_monitoring::Metrics,
    registry: Arc<SessionRegistry>,
    authenticator: Arc<PasswordAuthenticator>,
    slow_log: Arc<nodus_monitoring::SlowQueryLog>,
    tls: Option<Arc<tokio_rustls::TlsAcceptor>>,
    mut shutdown: tokio::sync::watch::Receiver<()>,
) -> anyhow::Result<()> {
    let mut param_provider = DefaultServerParameterProvider::default();
    param_provider.server_version = "16.0".to_string();

    let startup_handler = Arc::new(NodusStartupHandler {
        authenticator,
        param_provider,
        registry: registry.clone(),
    });
    let factory = Arc::new(NodusPgWireServer {
        startup_handler,
        executor: executor.clone(),
        metrics: metrics.clone(),
        registry: registry.clone(),
        slow_log: slow_log.clone(),
        extended_query_handler: Arc::new(NodusExtendedQueryHandler {
            executor,
            metrics: metrics.clone(),
            slow_log,
            registry: registry.clone(),
            cursors: RwLock::new(HashMap::new()),
        }),
        copy_handler: Arc::new(NodusCopyHandler {
            registry: registry.clone(),
        }),
    });

    info!(
        "PGWire server listening on {} (tls: {})",
        listener.local_addr()?,
        tls.is_some()
    );

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                info!("PGWire server shutting down...");
                break;
            }
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((socket, _)) => {
                        let factory = factory.clone();
                        let metrics = metrics.clone();
                        let tls = tls.clone();
                        tokio::spawn(async move {
                            metrics.pgwire_connections_total.inc();
                            metrics.active_sessions.inc();
                            if let Err(e) = process_nodus_socket(socket, tls, factory).await {
                                error!("Socket error: {}", e);
                            }
                            metrics.active_sessions.dec();
                        });
                    }
                    Err(e) => {
                        error!("PGWire accept error: {}", e);
                    }
                }
            }
        }
    }
    Ok(())
}
