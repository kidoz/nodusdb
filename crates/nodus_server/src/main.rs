use axum::Router;
use nodus_monitoring::{AppState, monitoring_routes};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!("Starting nodusd server...");

    let state = Arc::new(AppState::default());
    // Simulate initialization
    state
        .is_ready
        .store(true, std::sync::atomic::Ordering::Release);

    // Start PGWire server
    let pgwire_addr = "127.0.0.1:5432";
    tokio::spawn(async move {
        if let Err(e) = nodus_pgwire::start_pgwire_server(pgwire_addr).await {
            tracing::error!("PGWire server error: {}", e);
        }
    });

    let app = Router::new()
        .merge(monitoring_routes(state))
        .merge(nodus_web_console::web_console_routes());

    let addr = "127.0.0.1:8088";
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on http://{}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}
