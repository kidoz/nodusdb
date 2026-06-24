use nodus_config::NodusConfig;
use tokio::net::TcpListener;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

fn log_level(level: &str) -> Level {
    match level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Config layering: defaults <- TOML file (NODUS_CONFIG or ./nodus.toml) <- env.
    let config_path = std::env::var("NODUS_CONFIG").unwrap_or_else(|_| "nodus.toml".to_string());
    let config = NodusConfig::load(&config_path)?;

    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level(&config.observability.log_level))
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!("Starting nodusd server...");
    if config.tls.enabled {
        info!("TLS configured (termination wired separately)");
    }

    // Install OpenTelemetry trace export when an OTLP endpoint is configured.
    let _otel_provider = match &config.observability.otlp_endpoint {
        Some(endpoint) => {
            info!("Exporting traces via OTLP to {endpoint}");
            Some(nodus_telemetry::init_otlp(endpoint)?)
        }
        None => None,
    };

    let pgwire_listener = TcpListener::bind(&config.server.pgwire_addr).await?;
    let http_listener = TcpListener::bind(&config.server.http_addr).await?;

    info!(
        "Listening on http://{} and PGWire on {}",
        config.server.http_addr, config.server.pgwire_addr
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("Received Ctrl-C, shutting down gracefully...");
        let _ = shutdown_tx.send(());
    });

    let handle =
        nodus_server::run_server_with_config(pgwire_listener, http_listener, config, shutdown_rx)
            .await?;

    let _ = tokio::try_join!(handle.pgwire_task, handle.http_task)?;

    // Let the background loops (MVCC GC, WAL archiver) finish their current pass
    // after the shutdown signal instead of being abandoned mid-flight.
    for task in handle.background_tasks {
        let _ = task.await;
    }

    Ok(())
}
