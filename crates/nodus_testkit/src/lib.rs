#![allow(clippy::collapsible_if)]

pub mod cluster;
pub mod fault;

use nodus_security::SessionRegistry;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// Applies openraft's fast default Raft timers to a test config. Loopback has
/// none of the cold-connection/WAL latency the production defaults guard
/// against, so tests keep elections and failover quick (and avoid the slower
/// recovery the wider production window would add).
pub fn apply_fast_raft_timers(config: &mut nodus_config::NodusConfig) {
    config.cluster.raft_heartbeat_ms = 50;
    config.cluster.raft_election_timeout_min_ms = 150;
    config.cluster.raft_election_timeout_max_ms = 300;
}

pub struct TestServer {
    pub pgwire_addr: SocketAddr,
    pub http_addr: SocketAddr,
    pub registry: Arc<SessionRegistry>,
    #[allow(dead_code)]
    pgwire_task: JoinHandle<anyhow::Result<()>>,
    #[allow(dead_code)]
    http_task: JoinHandle<std::io::Result<()>>,
    /// GC, WAL archiver, Raft, etc. They hold the storage handles, so
    /// [`TestServer::shutdown`] must await them for the data dir to be fully
    /// released and flushed before another server reopens it.
    background_tasks: Vec<JoinHandle<()>>,
    shutdown_tx: tokio::sync::watch::Sender<()>,
}

impl TestServer {
    pub async fn start() -> anyhow::Result<Self> {
        let mut config = nodus_config::NodusConfig::default();
        config.admin.password = Some("nodus".into());
        // Test harness: allow the unauthenticated admin API (anonymous superuser)
        // so tests can call admin endpoints without wiring a token.
        config.admin.allow_insecure = true;
        apply_fast_raft_timers(&mut config);
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
            background_tasks: handle.background_tasks,
            shutdown_tx,
        })
    }

    /// Signals shutdown and waits for every server task — the listeners and the
    /// background loops holding the storage/Raft handles — to finish, so the
    /// data dir is fully flushed and released. Tests that restart a server on
    /// the same data directory must call this (rather than relying on `Drop`,
    /// which only signals and cannot await) to avoid racing the old server's
    /// teardown.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(());
        let _ = (&mut self.pgwire_task).await;
        let _ = (&mut self.http_task).await;
        for task in &mut self.background_tasks {
            let _ = task.await;
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(());
    }
}
