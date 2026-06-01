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

    let pgwire_listener = TcpListener::bind("127.0.0.1:5432").await?;
    let http_listener = TcpListener::bind("127.0.0.1:8088").await?;

    info!("Listening on http://127.0.0.1:8088 and PGWire on 127.0.0.1:5432");

    let handle = nodus_server::run_server(pgwire_listener, http_listener).await?;

    let _ = tokio::try_join!(handle.pgwire_task, handle.http_task)?;

    Ok(())
}
