//! In-process multi-node cluster fixture for distributed tests.
//!
//! Starts `N` real [`nodus_server`] instances inside one test process, each on
//! its own loopback ports and data directory. They form a cluster over real TCP
//! using the production code paths: the seed (node 1) bootstraps the `shard-meta`
//! Raft group, and nodes 2..=N join it via the hardened join protocol
//! (`/api/v1/cluster/join`). This is faster and far more controllable than
//! spawning OS subprocesses, and exercises the actual Raft network + join logic.
//!
//! Key detail: a node advertises its **HTTP address** for Raft, so the fixture
//! reserves a concrete port per node up front and binds it (with `SO_REUSEADDR`,
//! so a restart can reclaim it) — `raft_advertise_addr` must match the port the
//! HTTP listener is actually bound to.
//!
//! Caveat: the Raft *log* is currently in-memory (`NodusRaftStore`), so a stopped
//! node loses its consensus state; `restart` re-launches the process and reclaims
//! its ports, but full log-durable rejoin awaits a persistent log store. The
//! reliable, validated properties are **formation**, **replication**, and
//! **quorum fault-tolerance**.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use nodus_config::NodusConfig;
use nodus_server::{ServerHandle, run_server_with_config};
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpSocket};
use tokio::sync::watch;

/// Shared admin token; joining nodes present it as a bearer token to the seed.
const ADMIN_TOKEN: &str = "cluster-test-token";

/// Reserves a currently-free loopback port (bind ephemeral with `SO_REUSEADDR`,
/// read the assigned port, release it). The node rebinds the same port with
/// `SO_REUSEADDR`, so a restart can reclaim it even while it lingers in
/// `TIME_WAIT`.
fn reserve_port() -> Result<u16> {
    let sock = TcpSocket::new_v4()?;
    sock.set_reuseaddr(true)?;
    sock.bind("127.0.0.1:0".parse().unwrap())?;
    Ok(sock.local_addr()?.port())
}

/// Binds a listener on a specific loopback port with `SO_REUSEADDR`.
fn bind_reuse(port: u16) -> Result<TcpListener> {
    let sock = TcpSocket::new_v4()?;
    sock.set_reuseaddr(true)?;
    sock.bind(format!("127.0.0.1:{port}").parse().unwrap())?;
    Ok(sock.listen(1024)?)
}

struct Running {
    handle: ServerHandle,
    shutdown_tx: watch::Sender<()>,
}

/// One cluster member: fixed ports + a persistent data dir, plus its running
/// server (absent while stopped).
pub struct ClusterNode {
    pub node_id: u64,
    pub http_port: u16,
    pub pgwire_port: u16,
    /// Join peer (the seed's HTTP address) for non-seed nodes; `None` for the seed.
    seed_http: Option<String>,
    /// Kept alive so the data dir survives stop/restart; dropped with the node.
    _data_dir: TempDir,
    data_path: PathBuf,
    running: Option<Running>,
}

impl ClusterNode {
    pub fn http_addr(&self) -> String {
        format!("127.0.0.1:{}", self.http_port)
    }

    pub fn pgwire_addr(&self) -> SocketAddr {
        format!("127.0.0.1:{}", self.pgwire_port).parse().unwrap()
    }

    pub fn is_running(&self) -> bool {
        self.running.is_some()
    }

    fn config(&self) -> NodusConfig {
        let mut config = NodusConfig::default();
        config.admin.password = Some("nodus".into());
        config.admin.token = Some(ADMIN_TOKEN.into());
        config.cluster.node_id = self.node_id;
        config.cluster.raft_advertise_addr = self.http_addr();
        config.cluster.join_peers = self.seed_http.clone().into_iter().collect();
        config.storage.data_dir = Some(self.data_path.to_string_lossy().into_owned());
        config
    }

    async fn launch(&mut self) -> Result<()> {
        let pgwire_listener = bind_reuse(self.pgwire_port)?;
        let http_listener = bind_reuse(self.http_port)?;
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let handle =
            run_server_with_config(pgwire_listener, http_listener, self.config(), shutdown_rx)
                .await
                .with_context(|| format!("starting node {}", self.node_id))?;
        self.running = Some(Running {
            handle,
            shutdown_tx,
        });
        Ok(())
    }

    /// Signals shutdown and awaits the listener/background tasks so the ports are
    /// fully released before any restart reclaims them.
    async fn halt(&mut self) {
        if let Some(running) = self.running.take() {
            let _ = running.shutdown_tx.send(());
            let _ = running.handle.pgwire_task.await;
            let _ = running.handle.http_task.await;
            for task in running.handle.background_tasks {
                let _ = task.await;
            }
        }
    }
}

/// An `N`-node in-process cluster.
pub struct ClusterFixture {
    pub nodes: Vec<ClusterNode>,
    http: reqwest::Client,
}

impl ClusterFixture {
    /// Starts an `n`-node cluster (node 1 seeds; 2..=n join it) and waits until
    /// the seed reports all `n` nodes live.
    pub async fn start(n: usize) -> Result<Self> {
        assert!(n >= 1, "a cluster needs at least one node");
        let http = reqwest::Client::new();

        // Reserve all ports first so non-seed nodes can be told the seed's addr.
        let mut nodes = Vec::with_capacity(n);
        let mut seed_http = None;
        for i in 0..n {
            let http_port = reserve_port()?;
            let pgwire_port = reserve_port()?;
            if i == 0 {
                seed_http = Some(format!("127.0.0.1:{http_port}"));
            }
            let dir = tempfile::tempdir()?;
            let data_path = dir.path().to_path_buf();
            nodes.push(ClusterNode {
                node_id: (i + 1) as u64,
                http_port,
                pgwire_port,
                seed_http: if i == 0 { None } else { seed_http.clone() },
                _data_dir: dir,
                data_path,
                running: None,
            });
        }

        let mut fixture = Self { nodes, http };

        // Start the seed and wait for it to be *ready* — `/readyz` flips only
        // after it has elected itself leader AND bootstrapped the catalog (the
        // `default` database/schema), so queries issued afterward see it. The
        // hardened join backoff tolerates the joiners racing this regardless.
        fixture.nodes[0].launch().await?;
        if !fixture.wait_ready(0, Duration::from_secs(30)).await {
            anyhow::bail!("seed node did not become ready");
        }
        for i in 1..n {
            fixture.nodes[i].launch().await?;
        }

        if !fixture
            .wait_nodes_live(n as u32, Duration::from_secs(45))
            .await
        {
            anyhow::bail!("cluster did not reach {n}-node membership in time");
        }
        Ok(fixture)
    }

    /// Fetches a node's `/api/v1/cluster/overview` (the unauthenticated
    /// monitoring view).
    pub async fn overview(&self, idx: usize) -> Result<serde_json::Value> {
        let url = format!(
            "http://{}/api/v1/cluster/overview",
            self.nodes[idx].http_addr()
        );
        Ok(self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    /// Polls node `idx`'s overview until `pred` holds or `timeout` elapses.
    async fn wait_until(
        &self,
        idx: usize,
        pred: impl Fn(&serde_json::Value) -> bool,
        timeout: Duration,
    ) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Ok(ov) = self.overview(idx).await {
                if pred(&ov) {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        false
    }

    /// Waits until node `idx`'s `/readyz` reports ready (200) — for the seed this
    /// means the catalog has been bootstrapped.
    pub async fn wait_ready(&self, idx: usize, timeout: Duration) -> bool {
        let url = format!("http://{}/readyz", self.nodes[idx].http_addr());
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Ok(resp) = self.http.get(&url).send().await
                && resp.status().is_success()
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        false
    }

    /// Waits until the seed/leader (node 0) reports exactly `expected` live nodes.
    pub async fn wait_nodes_live(&self, expected: u32, timeout: Duration) -> bool {
        self.wait_until(
            0,
            |ov| ov.get("nodes_live").and_then(|v| v.as_u64()) == Some(expected as u64),
            timeout,
        )
        .await
    }

    /// Opens an authenticated pgwire client to node `idx`, retrying until the
    /// listener accepts.
    pub async fn pg_client(&self, idx: usize) -> Result<tokio_postgres::Client> {
        let conn_str = format!(
            "host=127.0.0.1 port={} user=nodus password=nodus dbname=default",
            self.nodes[idx].pgwire_port
        );
        for _ in 0..40 {
            if let Ok((client, connection)) =
                tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await
            {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                return Ok(client);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        anyhow::bail!("could not connect to node {idx} pgwire")
    }

    /// Single attempt to connect to node `idx` (no retry). Returns `None` if the
    /// listener isn't accepting (e.g. the node is stopped).
    async fn try_connect(&self, idx: usize) -> Option<tokio_postgres::Client> {
        let conn_str = format!(
            "host=127.0.0.1 port={} user=nodus password=nodus dbname=default",
            self.nodes[idx].pgwire_port
        );
        match tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                Some(client)
            }
            Err(_) => None,
        }
    }

    /// Runs `sql` against whichever of `candidates` is the current leader,
    /// retrying until one accepts the write or `timeout` elapses. A write to a
    /// non-leader is rejected fast (forward-to-leader), so this converges on the
    /// node that won the election. Returns the leader's index on success.
    ///
    /// Used by failover tests: after the original leader is killed, the survivors
    /// re-elect and exactly one starts accepting writes.
    pub async fn write_on_any(
        &self,
        candidates: &[usize],
        sql: &str,
        timeout: Duration,
    ) -> Option<usize> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            for &idx in candidates {
                if let Some(client) = self.try_connect(idx).await
                    && client.simple_query(sql).await.is_ok()
                {
                    return Some(idx);
                }
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        None
    }

    /// Stops node `idx` (graceful shutdown), releasing its ports. The data dir is
    /// retained so a later [`Self::restart`] reuses it.
    pub async fn stop(&mut self, idx: usize) -> Result<()> {
        self.nodes[idx].halt().await;
        Ok(())
    }

    /// Restarts a previously [`Self::stop`]ped node on the same ports + data dir.
    pub async fn restart(&mut self, idx: usize) -> Result<()> {
        if self.nodes[idx].is_running() {
            self.nodes[idx].halt().await;
        }
        self.nodes[idx].launch().await
    }
}

impl Drop for ClusterFixture {
    fn drop(&mut self) {
        // Signal every running node to wind down; tasks are detached after this
        // (we can't await in Drop), and TempDirs clean up the data dirs.
        for node in &self.nodes {
            if let Some(running) = &node.running {
                let _ = running.shutdown_tx.send(());
            }
        }
    }
}
