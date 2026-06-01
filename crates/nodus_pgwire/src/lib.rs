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

        // Minimal AST path
        match nodus_sql::parse_sql(query) {
            Ok(ast) => info!("Parsed AST: {:?}", ast),
            Err(e) => error!("Failed to parse SQL: {}", e),
        }

        let query_upper = query.trim().to_uppercase();
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

pub async fn start_pgwire_server(addr: &str) -> anyhow::Result<()> {
    let factory = Arc::new(NodusPgWireServer {
        startup_handler: Arc::new(NoopStartupHandler),
        simple_query_handler: Arc::new(NodusQueryHandler {
            session_state: nodus_sql::SessionState::default(),
        }),
        extended_query_handler: Arc::new(PlaceholderExtendedQueryHandler),
        copy_handler: Arc::new(NoopCopyHandler),
    });

    let listener = TcpListener::bind(addr).await?;
    info!("PGWire server listening on {}", addr);

    loop {
        let (socket, _) = listener.accept().await?;
        let factory = factory.clone();
        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, None, factory).await {
                error!("Socket error: {}", e);
            }
        });
    }
}
