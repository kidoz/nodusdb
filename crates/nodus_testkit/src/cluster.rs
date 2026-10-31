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
//! binds each initial listener before launching any nodes and holds it until the
//! corresponding server takes ownership. Restarts rebind the same ports with
//! `SO_REUSEADDR`; `raft_advertise_addr` always matches the actual HTTP listener.
//! Persistent data directories are retained across stop/restart.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use nodus_config::NodusConfig;
use nodus_server::{ServerHandle, run_server_with_config};
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpSocket};
use tokio::sync::watch;

/// Shared admin token; joining nodes present it as a bearer token to the seed,
/// and it authenticates the internal `/raft/...` ingress on the public listener.
pub const ADMIN_TOKEN: &str = "cluster-test-token";

/// Binds a listener on a specific loopback port with address/port reuse enabled
/// for persistent fixtures. Port zero asks the OS for a free port, which can be
/// read from `local_addr` while the listener remains bound.
fn bind_reuse(port: u16) -> Result<TcpListener> {
    let sock = TcpSocket::new_v4()?;
    sock.set_reuseaddr(true)?;
    #[cfg(unix)]
    sock.set_reuseport(true)?;
    sock.bind(format!("127.0.0.1:{port}").parse().unwrap())?;
    Ok(sock.listen(1024)?)
}

struct Running {
    handle: ServerHandle,
    shutdown_tx: watch::Sender<()>,
}

/// One cluster member: fixed ports + a persistent data dir, plus its running
/// server (absent while stopped).
/// Extra per-node configuration hook (see [`ClusterFixture::start_with`]).
type ConfigHook = std::sync::Arc<dyn Fn(&mut NodusConfig) + Send + Sync>;

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
    /// Extra configuration applied after the fixture defaults (e.g. a shared
    /// backup repository).
    configure: Option<ConfigHook>,
    /// Prebound dedicated raft peer listener (mTLS clusters), or None to share
    /// the HTTP listener.
    raft_listener: Option<TcpListener>,
    /// Permanent dedicated peer port, retained after the listener is consumed.
    raft_port: Option<u16>,
    /// Peer mTLS config (set for `start_cluster_mtls`).
    tls_config: Option<nodus_config::ClusterTlsConfig>,
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
        config.cluster.raft_advertise_addr = self
            .raft_port
            .map_or_else(|| self.http_addr(), |port| format!("127.0.0.1:{port}"));
        config.cluster.raft_listen_addr = self.raft_port.map(|port| format!("127.0.0.1:{port}"));
        config.cluster.join_peers = self.seed_http.clone().into_iter().collect();
        config.storage.data_dir = Some(self.data_path.to_string_lossy().into_owned());
        if let Some(tls) = &self.tls_config {
            config.cluster.tls = tls.clone();
        }
        crate::apply_fast_raft_timers(&mut config);
        if let Some(configure) = &self.configure {
            configure(&mut config);
        }
        config
    }

    async fn launch(&mut self) -> Result<()> {
        let pgwire_listener = bind_reuse(self.pgwire_port)
            .with_context(|| format!("binding pgwire port {}", self.pgwire_port))?;
        let http_listener = bind_reuse(self.http_port)
            .with_context(|| format!("binding HTTP port {}", self.http_port))?;
        if self.tls_config.is_some() && self.raft_listener.is_none() {
            let raft_addr = self.config().cluster.raft_advertise_addr;
            let raft_port = raft_addr.parse::<SocketAddr>()?.port();
            let mut retries = 0;
            let listener = loop {
                match bind_reuse(raft_port) {
                    Ok(listener) => break listener,
                    Err(error)
                        if error
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|error| error.kind() == std::io::ErrorKind::AddrInUse)
                            && retries < 100 =>
                    {
                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!("binding Raft peer port {raft_port} after {retries} retries")
                        });
                    }
                }
            };
            self.raft_listener = Some(listener);
        }
        self.launch_with_listeners(pgwire_listener, http_listener)
            .await
    }

    async fn launch_with_listeners(
        &mut self,
        pgwire_listener: TcpListener,
        http_listener: TcpListener,
    ) -> Result<()> {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let config = self.config();
        let raft_listener = self.raft_listener.take();
        let handle = if let Some(raft_listener) = raft_listener {
            nodus_server::run_server_full(
                pgwire_listener,
                http_listener,
                Some(raft_listener),
                config,
                shutdown_rx,
            )
            .await
        } else {
            run_server_with_config(pgwire_listener, http_listener, config, shutdown_rx).await
        }
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
        Self::start_inner(n, None, None).await
    }

    /// Like [`Self::start`], with a per-node configuration hook applied after
    /// the fixture defaults (e.g. a shared backup repository URI).
    pub async fn start_with(
        n: usize,
        configure: impl Fn(&mut NodusConfig) + Send + Sync + 'static,
    ) -> Result<Self> {
        let configure: ConfigHook = std::sync::Arc::new(configure);
        Self::start_inner(n, Some(configure), None).await
    }

    /// Starts an mTLS cluster: every node serves a dedicated raft peer listener
    /// with mandatory client-certificate TLS (`certs` = node cert, key, cluster
    /// CA from [`crate::write_test_certs`]). Admin/pgwire stay plain loopback.
    pub async fn start_cluster_mtls(
        n: usize,
        certs: &(String, String, String),
        configure: impl Fn(&mut NodusConfig) + Send + Sync + 'static,
    ) -> Result<Self> {
        let configure: ConfigHook = std::sync::Arc::new(configure);
        let tls = nodus_config::ClusterTlsConfig {
            enabled: true,
            cert_path: Some(certs.0.clone()),
            key_path: Some(certs.1.clone()),
            ca_path: Some(certs.2.clone()),
        };
        Self::start_inner(n, Some(configure), Some(tls)).await
    }

    async fn start_inner(
        n: usize,
        configure: Option<ConfigHook>,
        tls_config: Option<nodus_config::ClusterTlsConfig>,
    ) -> Result<Self> {
        assert!(n >= 1, "a cluster needs at least one node");
        let http = reqwest::Client::new();

        // Bind every initial listener before any server starts, so no other
        // process can claim an assigned port before its node takes ownership.
        let mut nodes = Vec::with_capacity(n);
        let mut listeners = Vec::with_capacity(n);
        let mut seed_http = None;
        for i in 0..n {
            let http_listener = bind_reuse(0)?;
            let http_port = http_listener.local_addr()?.port();
            let pgwire_listener = bind_reuse(0)?;
            let pgwire_port = pgwire_listener.local_addr()?.port();
            if i == 0 {
                seed_http = Some(format!("127.0.0.1:{http_port}"));
            }
            let dir = tempfile::tempdir()?;
            let data_path = dir.path().to_path_buf();
            let raft_listener = match &tls_config {
                Some(_) => Some(bind_reuse(0)?),
                None => None,
            };
            let raft_port = raft_listener
                .as_ref()
                .map(|listener| listener.local_addr().map(|addr| addr.port()))
                .transpose()?;
            nodes.push(ClusterNode {
                node_id: (i + 1) as u64,
                http_port,
                pgwire_port,
                seed_http: if i == 0 { None } else { seed_http.clone() },
                _data_dir: dir,
                data_path,
                running: None,
                configure: configure.clone(),
                raft_listener,
                raft_port,
                tls_config: tls_config.clone(),
            });
            listeners.push((pgwire_listener, http_listener));
        }

        let mut fixture = Self { nodes, http };

        // Start the seed and wait for it to be *ready* — `/readyz` flips only
        // after it has elected itself leader AND bootstrapped the catalog (the
        // `default` database/schema), so queries issued afterward see it. The
        // hardened join backoff tolerates the joiners racing this regardless.
        let (pgwire_listener, http_listener) = listeners.remove(0);
        fixture.nodes[0]
            .launch_with_listeners(pgwire_listener, http_listener)
            .await?;
        if !fixture.wait_ready(0, Duration::from_secs(30)).await {
            anyhow::bail!("seed node did not become ready");
        }
        for i in 1..n {
            let (pgwire_listener, http_listener) = listeners.remove(0);
            fixture.nodes[i]
                .launch_with_listeners(pgwire_listener, http_listener)
                .await?;
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

    /// Authenticated admin `POST` to node `idx`, returning the JSON body.
    pub async fn admin_post(&self, idx: usize, path: &str) -> Result<serde_json::Value> {
        let url = format!("http://{}{}", self.nodes[idx].http_addr(), path);
        Ok(self
            .http
            .post(url)
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    /// Authenticated admin `POST` with a JSON body to node `idx`, returning the
    /// JSON body.
    pub async fn admin_post_json(
        &self,
        idx: usize,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!("http://{}{}", self.nodes[idx].http_addr(), path);
        Ok(self
            .http
            .post(url)
            .bearer_auth(ADMIN_TOKEN)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    /// Authenticated admin `GET` from node `idx`, returning the JSON body.
    pub async fn admin_get(&self, idx: usize, path: &str) -> Result<serde_json::Value> {
        let url = format!("http://{}{}", self.nodes[idx].http_addr(), path);
        Ok(self
            .http
            .get(url)
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
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
