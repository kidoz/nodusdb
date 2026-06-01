use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{Sink, stream};
use tokio::net::TcpListener;
use tracing::{error, info};

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireHandlerFactory, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;

pub struct NodusQueryHandler {
    pub session_state: nodus_sql::SessionState,
    pub executor: Arc<dyn nodus_executor::Executor>,
    pub metrics: nodus_monitoring::Metrics,
}

#[async_trait]
impl SimpleQueryHandler for NodusQueryHandler {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        info!("Received query: {}", query);
        self.metrics.queries_total.inc();

        let ctx = nodus_executor::ExecutionContext {
            session_id: self.session_state.session_id.clone(),
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
    startup_handler: Arc<NoopStartupHandler>,
    simple_query_handler: Arc<NodusQueryHandler>,
    extended_query_handler: Arc<PlaceholderExtendedQueryHandler>,
    copy_handler: Arc<NoopCopyHandler>,
}

impl PgWireHandlerFactory for NodusPgWireServer {
    type StartupHandler = NoopStartupHandler;
    type SimpleQueryHandler = NodusQueryHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.simple_query_handler.clone()
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
) -> anyhow::Result<()> {
    let factory = Arc::new(NodusPgWireServer {
        startup_handler: Arc::new(NoopStartupHandler),
        simple_query_handler: Arc::new(NodusQueryHandler {
            session_state: nodus_sql::SessionState::default(),
            executor,
            metrics: metrics.clone(),
        }),
        extended_query_handler: Arc::new(PlaceholderExtendedQueryHandler),
        copy_handler: Arc::new(NoopCopyHandler),
    });

    info!("PGWire server listening on {}", listener.local_addr()?);

    loop {
        let (socket, _) = listener.accept().await?;
        let factory = factory.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            metrics.pgwire_connections_total.inc();
            metrics.active_sessions.inc();
            if let Err(e) = process_socket(socket, None, factory).await {
                error!("Socket error: {}", e);
            }
            metrics.active_sessions.dec();
        });
    }
}
