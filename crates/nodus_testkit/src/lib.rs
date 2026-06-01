pub mod fault;

use nodus_security::SessionRegistry;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub struct TestServer {
    pub pgwire_addr: SocketAddr,
    pub http_addr: SocketAddr,
    pub registry: Arc<SessionRegistry>,
    #[allow(dead_code)]
    pgwire_task: JoinHandle<anyhow::Result<()>>,
    #[allow(dead_code)]
    http_task: JoinHandle<std::io::Result<()>>,
    shutdown_tx: tokio::sync::watch::Sender<()>,
}

impl TestServer {
    pub async fn start() -> anyhow::Result<Self> {
        Self::start_with_config(nodus_config::NodusConfig::default()).await
    }

    pub async fn start_with_config(config: nodus_config::NodusConfig) -> anyhow::Result<Self> {
        let pgwire_listener = TcpListener::bind("127.0.0.1:0").await?;
        let http_listener = TcpListener::bind("127.0.0.1:0").await?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

        let handle = nodus_server::run_server_with_config(
            pgwire_listener,
            http_listener,
            config,
            shutdown_rx,
        )
        .await?;

        Ok(Self {
            pgwire_addr: handle.pgwire_addr,
            http_addr: handle.http_addr,
            registry: handle.registry,
            pgwire_task: handle.pgwire_task,
            http_task: handle.http_task,
            shutdown_tx,
        })
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(());
    }
}
