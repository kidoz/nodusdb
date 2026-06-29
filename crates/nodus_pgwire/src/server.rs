//! TCP server and connection lifecycle.
//!
//! Each accepted connection is driven through pgwire's [`process_socket`], which
//! owns SSL/GSSENC negotiation, the startup timeout, the frontend-message loop,
//! and cancel-request handling. NodusDB contributes only:
//!   - a per-connection [`NodusHandlers`] factory wiring in its query/extended/
//!     copy/startup/cancel handlers,
//!   - session registration, which happens inside the startup handler, and
//!   - post-loop cleanup (deregister + `end_session`) keyed by the session id the
//!     startup handler records in the shared [`ConnState`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use nodus_security::{PasswordAuthenticator, SessionRegistry};
use pgwire::api::PgWireServerHandlers;
use pgwire::api::auth::{DefaultServerParameterProvider, StartupHandler};
use pgwire::api::cancel::CancelHandler;
use pgwire::api::copy::CopyHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::messages::cancel::CancelRequest;
use pgwire::messages::startup::SecretKey;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::NodusQueryHandler;
use crate::copy::NodusCopyHandler;
use crate::extended_query::NodusExtendedQueryHandler;
use crate::startup::NodusStartupHandler;

/// Shared, connection-independent server state. Built once; cloned (by Arc) into
/// every connection's handler factory.
pub(crate) struct NodusShared {
    pub(crate) executor: Arc<dyn nodus_executor::Executor>,
    pub(crate) metrics: nodus_monitoring::Metrics,
    pub(crate) registry: Arc<SessionRegistry>,
    pub(crate) slow_log: Arc<nodus_monitoring::SlowQueryLog>,
    pub(crate) authenticator: Arc<PasswordAuthenticator>,
    pub(crate) param_provider: Arc<DefaultServerParameterProvider>,
    /// The extended-query and COPY handlers are stateless across connections
    /// (their per-session state is keyed by session id), so a single instance is
    /// shared; the simple-query and startup handlers are built per connection.
    pub(crate) extended_query_handler: Arc<NodusExtendedQueryHandler>,
    pub(crate) copy_handler: Arc<NodusCopyHandler>,
}

/// Per-connection state. The startup handler records the connection's session id
/// here once it registers it, so the post-`process_socket` cleanup can release
/// the session even though pgwire owns the connection loop.
#[derive(Default)]
pub(crate) struct ConnState {
    pub(crate) session_id: Mutex<Option<String>>,
}

/// Per-connection handler factory handed to [`process_socket`]. Cheap to create
/// (holds only `Arc`s); a fresh one is made for every accepted connection.
struct NodusHandlers {
    shared: Arc<NodusShared>,
    conn: Arc<ConnState>,
}

impl PgWireServerHandlers for NodusHandlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(NodusQueryHandler {
            session_id: String::new(),
            session_state: nodus_sql::SessionState::default(),
            executor: self.shared.executor.clone(),
            metrics: self.shared.metrics.clone(),
            registry: self.shared.registry.clone(),
            slow_log: self.shared.slow_log.clone(),
        })
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.shared.extended_query_handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(NodusStartupHandler::new(
            self.shared.authenticator.clone(),
            self.shared.param_provider.clone(),
            self.shared.registry.clone(),
            self.conn.clone(),
        ))
    }

    fn copy_handler(&self) -> Arc<impl CopyHandler> {
        self.shared.copy_handler.clone()
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::new(NodusCancelHandler {
            registry: self.shared.registry.clone(),
        })
    }
}

/// Handles a PostgreSQL `CancelRequest` (sent on a fresh, dedicated connection):
/// it carries the target backend's pid and secret key, which we match against
/// the registered backend keys to cancel that backend's in-flight query.
struct NodusCancelHandler {
    registry: Arc<SessionRegistry>,
}

#[async_trait]
impl CancelHandler for NodusCancelHandler {
    async fn on_cancel_request(&self, cancel: CancelRequest) {
        let secret = secret_key_to_i32(&cancel.secret_key);
        let accepted = self.registry.cancel_backend_query(cancel.pid, secret);
        info!(
            "Received cancel request for backend pid={} accepted={}",
            cancel.pid, accepted
        );
    }
}

/// Extracts the i32 secret NodusDB registers (it always issues `SecretKey::I32`),
/// tolerating the byte form a client might echo back.
fn secret_key_to_i32(key: &SecretKey) -> i32 {
    match key {
        SecretKey::I32(v) => *v,
        SecretKey::Bytes(b) if b.len() >= 4 => i32::from_be_bytes([b[0], b[1], b[2], b[3]]),
        _ => 0,
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
    max_connections: usize,
    mut shutdown: tokio::sync::watch::Receiver<()>,
) -> anyhow::Result<()> {
    let mut param_provider = DefaultServerParameterProvider::default();
    param_provider.server_version = "18.0".to_string();
    let param_provider = Arc::new(param_provider);

    let shared = Arc::new(NodusShared {
        executor: executor.clone(),
        metrics: metrics.clone(),
        registry: registry.clone(),
        slow_log: slow_log.clone(),
        authenticator,
        param_provider,
        extended_query_handler: Arc::new(NodusExtendedQueryHandler {
            executor: executor.clone(),
            metrics: metrics.clone(),
            slow_log,
            registry: registry.clone(),
            cursors: RwLock::new(HashMap::new()),
        }),
        copy_handler: Arc::new(NodusCopyHandler::new(registry, executor)),
    });

    // Bound concurrent connections so an unauthenticated client cannot exhaust
    // memory/file descriptors by opening unlimited sockets; excess connections
    // are refused (closed) rather than each spawning an unbounded task.
    let max_connections = max_connections.max(1);
    let conn_limiter = Arc::new(tokio::sync::Semaphore::new(max_connections));

    info!(
        "PGWire server listening on {} (tls: {}, max_connections: {})",
        listener.local_addr()?,
        tls.is_some(),
        max_connections,
    );

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                info!("PGWire server shutting down...");
                break;
            }
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((socket, peer)) => {
                        let permit = match conn_limiter.clone().try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                warn!(
                                    "refusing connection from {peer}: max_connections ({max_connections}) reached"
                                );
                                drop(socket);
                                continue;
                            }
                        };
                        let shared = shared.clone();
                        let metrics = metrics.clone();
                        // pgwire takes a `tokio_rustls::TlsAcceptor` by value.
                        let tls_acceptor = tls.as_ref().map(|a| (**a).clone());
                        tokio::spawn(async move {
                            // Held for the connection's lifetime; a slot frees on drop.
                            let _permit = permit;
                            metrics.pgwire_connections_total.inc();
                            metrics.active_sessions.inc();

                            let conn = Arc::new(ConnState::default());
                            let handlers = NodusHandlers {
                                shared: shared.clone(),
                                conn: conn.clone(),
                            };
                            if let Err(e) = process_socket(socket, tls_acceptor, handlers).await {
                                error!("Socket error: {}", e);
                            }

                            // Release the session the startup handler registered.
                            // `end_session` aborts any open transaction and routes
                            // through the Raft router, so run it off the reactor.
                            // Take the id first so the lock guard isn't held across
                            // the await (it is not `Send`).
                            let session_id = conn.session_id.lock().unwrap().take();
                            if let Some(session_id) = session_id {
                                shared.registry.deregister(&session_id);
                                let executor = shared.executor.clone();
                                let _ = tokio::task::spawn_blocking(move || {
                                    executor.end_session(&session_id)
                                })
                                .await;
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
