mod admin;

use admin::{AdminState, admin_routes};
use axum::Router;
use nodus_catalog::{
    CatalogWriter, CreateRoleRequest, GrantPrivilegeRequest, PrincipalType, ResourceRef,
};
use nodus_monitoring::{AppState, monitoring_routes};
use nodus_security::{PasswordAuthenticator, SessionRegistry};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tower_http::cors::{Any, CorsLayer};

pub struct ServerHandle {
    pub pgwire_addr: SocketAddr,
    pub http_addr: SocketAddr,
    pub pgwire_task: JoinHandle<anyhow::Result<()>>,
    pub http_task: JoinHandle<std::io::Result<()>>,
    /// Shared registry of active client sessions (inspection + cancellation).
    pub registry: Arc<SessionRegistry>,
}

pub async fn run_server(
    pgwire_listener: TcpListener,
    http_listener: TcpListener,
) -> anyhow::Result<ServerHandle> {
    let pgwire_addr = pgwire_listener.local_addr()?;
    let http_addr = http_listener.local_addr()?;

    let state = Arc::new(AppState::default());
    state
        .is_ready
        .store(true, std::sync::atomic::Ordering::Release);

    // Shared catalog so the authenticator's principals and the executor's
    // authorization grants resolve against the same data. Audit events go to an
    // in-memory sink that the admin audit API can query (a durable file sink is
    // selectable via config separately).
    let audit = Arc::new(nodus_audit::MemoryAuditSink::new());
    let (executor, catalog) = nodus_executor::MemExecutor::shared(audit.clone());
    let admin = catalog
        .create_role(CreateRoleRequest {
            name: "nodus".into(),
            principal_type: PrincipalType::User,
            database_id: None,
        })
        .map_err(|e| anyhow::anyhow!("seed admin: {e}"))?;
    // Bootstrap superuser: ALL on System bypasses per-resource grant checks.
    catalog
        .grant_privilege(GrantPrivilegeRequest {
            principal_id: admin.id,
            resource: ResourceRef::System,
            privilege: "ALL".into(),
        })
        .map_err(|e| anyhow::anyhow!("seed admin grant: {e}"))?;
    // A read-only authz engine over the same catalog for the admin explain API.
    let authz = Arc::new(nodus_authz::DefaultAuthzEngine::new(catalog.clone()));
    let authenticator = Arc::new(PasswordAuthenticator::new(catalog.clone()));
    // Default development credentials; override before production use.
    authenticator.set_password("nodus", admin.id, "nodus");

    let registry = Arc::new(SessionRegistry::new());
    let pgwire_metrics = state.metrics.clone();
    let pgwire_registry = registry.clone();
    let pgwire_task = tokio::spawn(async move {
        nodus_pgwire::start_pgwire_server(
            pgwire_listener,
            executor,
            pgwire_metrics,
            pgwire_registry,
            authenticator,
        )
        .await
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let admin_state = AdminState {
        registry: registry.clone(),
        audit: audit.clone(),
        authz: authz.clone(),
        catalog: catalog.clone(),
    };
    let app = Router::new()
        .merge(monitoring_routes(state))
        .merge(admin_routes(admin_state))
        .merge(nodus_web_console::web_console_routes())
        .layer(cors);

    let http_task = tokio::spawn(async move { axum::serve(http_listener, app).await });

    Ok(ServerHandle {
        pgwire_addr,
        http_addr,
        pgwire_task,
        http_task,
        registry,
    })
}
