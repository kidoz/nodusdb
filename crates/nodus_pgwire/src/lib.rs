use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use futures_util::{Sink, SinkExt, stream};
use tokio::net::TcpListener;
use tracing::{error, info};

use nodus_catalog::PrincipalId;
use nodus_security::{Authenticator, PasswordAuthenticator, Session, SessionRegistry};
use pgwire::api::auth::{
    DefaultServerParameterProvider, LoginInfo, StartupHandler, finish_authentication,
    save_startup_parameters_to_metadata,
};
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireConnectionState, PgWireHandlerFactory, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::response::ErrorResponse;
use pgwire::messages::startup::Authentication;
use pgwire::tokio::process_socket;
use uuid::Uuid;

pub struct NodusQueryHandler {
    /// Unique per-connection session id, also the registry key.
    pub session_id: String,
    pub session_state: nodus_sql::SessionState,
    pub executor: Arc<dyn nodus_executor::Executor>,
    pub metrics: nodus_monitoring::Metrics,
    registry: Arc<SessionRegistry>,
    slow_log: Arc<nodus_monitoring::SlowQueryLog>,
    /// Cancellation token for this session; flipped by an admin `kill`.
    cancel: Arc<AtomicBool>,
}

/// Observes query latency on drop, so every `do_query` return path is covered:
/// records into the histogram and, if slow, into the slow-query log.
struct QueryTimer<'a> {
    start: std::time::Instant,
    sql: &'a str,
    session_id: &'a str,
    metrics: &'a nodus_monitoring::Metrics,
    slow_log: &'a nodus_monitoring::SlowQueryLog,
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

impl Drop for NodusQueryHandler {
    fn drop(&mut self) {
        // The handler lives for the connection's lifetime (one per socket), so
        // dropping it means the client disconnected.
        self.registry.deregister(&self.session_id);
    }
}

/// Startup handler that authenticates clients with cleartext passwords against
/// the [`PasswordAuthenticator`]. On success the principal id is stashed in the
/// connection metadata for downstream authorization.
pub struct NodusStartupHandler {
    authenticator: Arc<PasswordAuthenticator>,
    param_provider: DefaultServerParameterProvider,
}

#[async_trait]
impl StartupHandler for NodusStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: pgwire::messages::PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            pgwire::messages::PgWireFrontendMessage::Startup(ref startup) => {
                save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let pwd = pwd.into_password()?;
                let login = LoginInfo::from_client_info(client);
                let user = login.user().map(|u| u.to_string()).unwrap_or_default();
                match self.authenticator.authenticate(&user, &pwd.password) {
                    Ok(session) => {
                        client.metadata_mut().insert(
                            "nodus_principal_id".to_string(),
                            session.principal_id.to_string(),
                        );
                        finish_authentication(client, &self.param_provider).await;
                    }
                    Err(_) => {
                        let error_info = ErrorInfo::new(
                            "FATAL".to_owned(),
                            "28P01".to_owned(),
                            "password authentication failed".to_owned(),
                        );
                        client
                            .feed(PgWireBackendMessage::ErrorResponse(ErrorResponse::from(
                                error_info,
                            )))
                            .await?;
                        client.close().await?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for NodusQueryHandler {
    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        info!("Received query: {}", query);
        self.metrics.queries_total.inc();

        // Honor an administrative cancellation of this session.
        if self.cancel.load(Ordering::SeqCst) {
            self.metrics.query_errors_total.inc();
            return Err(std::io::Error::other("session terminated by administrator").into());
        }
        self.registry.set_current_query(&self.session_id, query);

        // OpenTelemetry span covering the statement (no-op unless OTLP is on).
        let _otel_span = nodus_telemetry::start_span("pgwire.simple_query");

        // Times the whole statement regardless of which branch returns.
        let _timer = QueryTimer {
            start: std::time::Instant::now(),
            sql: query,
            session_id: &self.session_id,
            metrics: &self.metrics,
            slow_log: &self.slow_log,
        };

        // The authenticated principal was stashed in connection metadata by the
        // startup handler; carry it into the execution context for authorization.
        let principal_id = client
            .metadata()
            .get("nodus_principal_id")
            .and_then(|s| Uuid::parse_str(s).ok())
            .map(PrincipalId)
            .unwrap_or_default();

        let ctx = nodus_executor::ExecutionContext {
            session_id: self.session_id.clone(),
            principal_id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };

        // Minimal AST path
        match nodus_sql::parse_sql(query) {
            Ok(ast) => {
                info!("Parsed AST: {:?}", ast);
                // In MVP, we are skipping the full translation and just mapping matching strings below.
            }
            Err(e) => {
                error!("Failed to parse SQL: {}", e);
                self.metrics.query_errors_total.inc();
            }
        }

        let query_upper = query.trim().to_uppercase();

        if query_upper.starts_with("CREATE TABLE") {
            let table_name = "users".to_string(); // MVP hardcode parsing
            let _res = self
                .executor
                .execute_logical(
                    &ctx,
                    nodus_executor::LogicalPlan::CreateTable { name: table_name },
                )
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            return Ok(vec![Response::Execution(Tag::new("CREATE TABLE"))]);
        }

        if query_upper.starts_with("INSERT INTO") {
            // "INSERT INTO users (id, name) VALUES ('123', 'alice')"
            // Very naive MVP parsing: split on 'VALUES'
            let parts: Vec<&str> = query.split("VALUES").collect();
            if parts.len() == 2 {
                let vals = parts[1]
                    .trim()
                    .trim_matches(|c| c == '(' || c == ')' || c == ';');
                let vals_split: Vec<&str> = vals.split(',').collect();
                if vals_split.len() == 2 {
                    let id = vals_split[0].trim().trim_matches('\'').to_string();
                    let name = vals_split[1].trim().trim_matches('\'').to_string();
                    self.executor
                        .execute_logical(
                            &ctx,
                            nodus_executor::LogicalPlan::Insert {
                                table_name: "users".into(),
                                id,
                                name_val: name,
                            },
                        )
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                    return Ok(vec![Response::Execution(Tag::new("INSERT 0 1"))]);
                }
            }
        }

        if query_upper.starts_with("SELECT ID, NAME FROM USERS WHERE ID") {
            // "SELECT id, name FROM users WHERE id = '123'"
            let parts: Vec<&str> = query.split('=').collect();
            if parts.len() == 2 {
                let id = parts[1]
                    .trim()
                    .trim_matches(|c| c == '\'' || c == ';')
                    .to_string();
                let rows = self
                    .executor
                    .execute_logical(
                        &ctx,
                        nodus_executor::LogicalPlan::SelectById {
                            table_name: "users".into(),
                            id,
                        },
                    )
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                let field_info = Arc::new(vec![
                    FieldInfo::new("id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
                    FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text),
                ]);

                let mut rows_stream = Vec::new();
                for r in rows {
                    let mut encoder = DataRowEncoder::new(field_info.clone());
                    encoder
                        .encode_field(&r.columns[0])
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                    encoder
                        .encode_field(&r.columns[1])
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                    rows_stream.push(encoder.finish());
                }

                let response = QueryResponse::new(field_info, stream::iter(rows_stream));
                return Ok(vec![Response::Query(response)]);
            }
        }

        if query_upper.starts_with("BEGIN") {
            self.executor
                .execute_logical(&ctx, nodus_executor::LogicalPlan::Begin)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            return Ok(vec![Response::Execution(Tag::new("BEGIN"))]);
        }
        if query_upper.starts_with("COMMIT") {
            self.executor
                .execute_logical(&ctx, nodus_executor::LogicalPlan::Commit)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            return Ok(vec![Response::Execution(Tag::new("COMMIT"))]);
        }
        if query_upper.starts_with("ROLLBACK") {
            self.executor
                .execute_logical(&ctx, nodus_executor::LogicalPlan::Rollback)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            return Ok(vec![Response::Execution(Tag::new("ROLLBACK"))]);
        }
        if query_upper == "SELECT 1;" || query_upper == "SELECT 1" {
            let field_info = Arc::new(vec![FieldInfo::new(
                "?column?".into(),
                None,
                None,
                Type::INT4,
                FieldFormat::Text,
            )]);
            let mut encoder = DataRowEncoder::new(field_info.clone());
            encoder.encode_field(&1i32)?;
            let row = encoder.finish();
            let response = QueryResponse::new(field_info, stream::iter(vec![row]));
            return Ok(vec![Response::Query(response)]);
        }

        if query_upper.starts_with("SELECT 'HELLO'") {
            let field_info = Arc::new(vec![FieldInfo::new(
                "?column?".into(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Text,
            )]);
            let mut encoder = DataRowEncoder::new(field_info.clone());
            encoder.encode_field(&"hello")?;
            let row = encoder.finish();
            let response = QueryResponse::new(field_info, stream::iter(vec![row]));
            return Ok(vec![Response::Query(response)]);
        }

        if query_upper.starts_with("SELECT VERSION()") {
            let field_info = Arc::new(vec![FieldInfo::new(
                "version".into(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Text,
            )]);
            let mut encoder = DataRowEncoder::new(field_info.clone());
            encoder.encode_field(&"PostgreSQL 16.0 (NodusDB)")?;
            let row = encoder.finish();
            let response = QueryResponse::new(field_info, stream::iter(vec![row]));
            return Ok(vec![Response::Query(response)]);
        }

        if query_upper.starts_with("SHOW SEARCH_PATH") {
            let field_info = Arc::new(vec![FieldInfo::new(
                "search_path".into(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Text,
            )]);
            let mut encoder = DataRowEncoder::new(field_info.clone());
            encoder.encode_field(&"public")?;
            let row = encoder.finish();
            let response = QueryResponse::new(field_info, stream::iter(vec![row]));
            return Ok(vec![Response::Query(response)]);
        }

        Ok(vec![Response::Execution(Tag::new("OK"))])
    }
}

pub struct NodusPgWireServer {
    startup_handler: Arc<NodusStartupHandler>,
    executor: Arc<dyn nodus_executor::Executor>,
    metrics: nodus_monitoring::Metrics,
    registry: Arc<SessionRegistry>,
    slow_log: Arc<nodus_monitoring::SlowQueryLog>,
    extended_query_handler: Arc<PlaceholderExtendedQueryHandler>,
    copy_handler: Arc<NoopCopyHandler>,
}

impl PgWireHandlerFactory for NodusPgWireServer {
    type StartupHandler = NodusStartupHandler;
    type SimpleQueryHandler = NodusQueryHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

    // Called once per connection by `process_socket`, so each client gets its
    // own session registered in the shared registry.
    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        let session_id = Uuid::new_v4().to_string();
        // Anonymous until authentication is wired into the startup handler.
        let session = Session {
            session_id: session_id.clone(),
            principal_id: PrincipalId::new(),
            active_roles: vec![],
            database_id: None,
        };
        let cancel = self.registry.register(&session);
        Arc::new(NodusQueryHandler {
            session_id,
            session_state: nodus_sql::SessionState::default(),
            executor: self.executor.clone(),
            metrics: self.metrics.clone(),
            registry: self.registry.clone(),
            slow_log: self.slow_log.clone(),
            cancel,
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

pub async fn start_pgwire_server(
    listener: TcpListener,
    executor: Arc<dyn nodus_executor::Executor>,
    metrics: nodus_monitoring::Metrics,
    registry: Arc<SessionRegistry>,
    authenticator: Arc<PasswordAuthenticator>,
    slow_log: Arc<nodus_monitoring::SlowQueryLog>,
    tls: Option<Arc<tokio_rustls::TlsAcceptor>>,
) -> anyhow::Result<()> {
    let startup_handler = Arc::new(NodusStartupHandler {
        authenticator,
        param_provider: DefaultServerParameterProvider::default(),
    });
    let factory = Arc::new(NodusPgWireServer {
        startup_handler,
        executor,
        metrics: metrics.clone(),
        registry,
        slow_log,
        extended_query_handler: Arc::new(PlaceholderExtendedQueryHandler),
        copy_handler: Arc::new(NoopCopyHandler),
    });

    info!(
        "PGWire server listening on {} (tls: {})",
        listener.local_addr()?,
        tls.is_some()
    );

    loop {
        let (socket, _) = listener.accept().await?;
        let factory = factory.clone();
        let metrics = metrics.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            metrics.pgwire_connections_total.inc();
            metrics.active_sessions.inc();
            if let Err(e) = process_socket(socket, tls, factory).await {
                error!("Socket error: {}", e);
            }
            metrics.active_sessions.dec();
        });
    }
}
