use axum::Router;
use nodus_monitoring::{AppState, monitoring_routes};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub struct ServerHandle {
    pub pgwire_addr: SocketAddr,
    pub http_addr: SocketAddr,
    pub pgwire_task: JoinHandle<anyhow::Result<()>>,
    pub http_task: JoinHandle<std::io::Result<()>>,
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

    let pgwire_task =
        tokio::spawn(async move { nodus_pgwire::start_pgwire_server(pgwire_listener).await });

    let app = Router::new()
        .merge(monitoring_routes(state))
        .merge(nodus_web_console::web_console_routes());

    let http_task = tokio::spawn(async move { axum::serve(http_listener, app).await });

    Ok(ServerHandle {
        pgwire_addr,
        http_addr,
        pgwire_task,
        http_task,
    })
}
