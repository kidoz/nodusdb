#![allow(clippy::collapsible_if)]

pub mod cluster;
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
        let mut config = nodus_config::NodusConfig::default();
        config.admin.password = Some("nodus".into());
        Self::start_with_config(config).await
    }

    pub async fn start_with_config(config: nodus_config::NodusConfig) -> anyhow::Result<Self> {
        let pgwire_listener = TcpListener::bind("127.0.0.1:0").await?;
        let http_listener = TcpListener::bind("127.0.0.1:0").await?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

        // TLS is a single global config applied to BOTH the pgwire and admin
        // HTTP listeners, so when it's on the readiness probe must speak HTTPS
        // (and accept the throwaway self-signed test cert).
        let tls_enabled = config.tls.enabled;

        let handle = nodus_server::run_server_with_config(
            pgwire_listener,
            http_listener,
            config,
            shutdown_rx,
        )
        .await?;

        // Wait for the server to be ready before returning
        let scheme = if tls_enabled { "https" } else { "http" };
        let client = if tls_enabled {
            reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
                .build()?
        } else {
            reqwest::Client::new()
        };
        let readyz_url = format!("{scheme}://127.0.0.1:{}/readyz", handle.http_addr.port());
        let mut ready = false;
        for _ in 0..50 {
            if let Ok(resp) = client.get(&readyz_url).send().await {
                if resp.status().is_success() {
                    ready = true;
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        if !ready {
            anyhow::bail!("Server did not become ready in time");
        }

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
