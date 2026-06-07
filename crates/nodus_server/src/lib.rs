mod admin;
mod raft_kv;
mod raft_catalog;
mod raft_upgrade;

use admin::{AdminState, admin_routes};
use axum::Router;
use nodus_backup::{BackupOrchestrator, FsBackupRepository};
use nodus_catalog::{
    CatalogReader, CatalogWriter, CreateRoleRequest, GrantPrivilegeRequest, PrincipalType,
    ResourceRef,
};
use nodus_config::NodusConfig;
use nodus_monitoring::{AppState, monitoring_routes};
use nodus_security::{PasswordAuthenticator, SessionRegistry};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::RootCertStore;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tower_http::cors::{Any, CorsLayer};

pub struct ServerHandle {
    pub pgwire_addr: SocketAddr,
    pub http_addr: SocketAddr,
    pub pgwire_task: JoinHandle<anyhow::Result<()>>,
    pub http_task: JoinHandle<std::io::Result<()>>,
    /// Shared registry of active client sessions (inspection + cancellation).
    pub registry: Arc<SessionRegistry>,
}

/// Resolves a backup repository directory from a `file://` URI, falling back to
/// a unique temp directory when unset so the backup API is usable in dev/tests.
fn backup_dir(uri: &str) -> PathBuf {
    let trimmed = uri.strip_prefix("file://").unwrap_or(uri);
    if trimmed.is_empty() {
        std::env::temp_dir().join(format!("nodus-backups-{}", uuid::Uuid::new_v4()))
    } else {
        PathBuf::from(trimmed)
    }
}

/// Builds a TLS acceptor from configuration. Returns `None` when TLS is
/// disabled. Errors if enabled but the cert/key are missing or invalid.
fn load_tls_config(tls: &nodus_config::TlsConfig) -> anyhow::Result<Option<Arc<ServerConfig>>> {
    if !tls.enabled {
        return Ok(None);
    }
    let cert_path = tls
        .cert_path
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("tls.enabled but cert_path is unset"))?;
    let key_path = tls
        .key_path
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("tls.enabled but key_path is unset"))?;

    // Ensure a process-wide crypto provider is available (aws-lc-rs, as enabled
    // by pgwire). Ignored if one is already installed.
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();

    let cert_bytes = std::fs::read(cert_path)?;
    let certs: Vec<CertificateDer> =
        rustls_pemfile::certs(&mut cert_bytes.as_slice()).collect::<Result<_, _>>()?;
    let key_bytes = std::fs::read(key_path)?;
    let key: PrivateKeyDer = rustls_pemfile::private_key(&mut key_bytes.as_slice())?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {key_path}"))?;

    let client_auth = if let Some(ca_path) = &tls.client_ca_path {
        let ca_bytes = std::fs::read(ca_path)?;
        let mut root_cert_store = RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut ca_bytes.as_slice()) {
            root_cert_store.add(cert?)?;
        }
        if tls.require_client_auth {
            WebPkiClientVerifier::builder(root_cert_store.into()).build()?
        } else {
            WebPkiClientVerifier::builder(root_cert_store.into())
                .allow_unauthenticated()
                .build()?
        }
    } else {
        WebPkiClientVerifier::no_client_auth()
    };

    let config = ServerConfig::builder()
        .with_client_cert_verifier(client_auth)
        .with_single_cert(certs, key)?;
    Ok(Some(Arc::new(config)))
}

/// Starts the server with default configuration.
pub async fn run_server(
    pgwire_listener: TcpListener,
    http_listener: TcpListener,
    shutdown: tokio::sync::watch::Receiver<()>,
) -> anyhow::Result<ServerHandle> {
    run_server_with_config(
        pgwire_listener,
        http_listener,
        NodusConfig::default(),
        shutdown,
    )
    .await
}

pub async fn run_server_with_config(
    pgwire_listener: TcpListener,
    http_listener: TcpListener,
    config: NodusConfig,
    mut shutdown: tokio::sync::watch::Receiver<()>,
) -> anyhow::Result<ServerHandle> {
    let pgwire_addr = pgwire_listener.local_addr()?;
    let http_addr = http_listener.local_addr()?;

    let state = Arc::new(AppState::default());
    state
        .is_ready
        .store(false, std::sync::atomic::Ordering::Release);

    // Shared catalog so the authenticator's principals and the executor's
    // authorization grants resolve against the same data. The audit sink is
    // durable (JSONL file) when configured, else in-memory; the same object
    // backs both executor emission and the admin audit query API.
    let (audit_sink, audit_query): (
        Arc<dyn nodus_audit::AuditSink>,
        Arc<dyn nodus_audit::AuditQueryable>,
    ) = match &config.audit.file_path {
        Some(path) => {
            let sink = Arc::new(nodus_audit::FileAuditSink::new(path));
            (sink.clone(), sink)
        }
        None => {
            let sink = Arc::new(nodus_audit::MemoryAuditSink::new());
            (sink.clone(), sink)
        }
    };
    let encryption_key = if let Some(hex_key) = &config.storage.encryption_key {
        let bytes = hex::decode(hex_key)
            .map_err(|e| anyhow::anyhow!("invalid encryption_key hex: {}", e))?;
        let mut key = [0u8; 32];
        if bytes.len() != 32 {
            return Err(anyhow::anyhow!(
                "encryption_key must be exactly 32 bytes (64 hex characters)"
            ));
        }
        key.copy_from_slice(&bytes);
        Some(key)
    } else {
        None
    };

    println!("Loading KV and catalog");
    let (local_kv, catalog): (Arc<dyn nodus_storage_api::KvEngine>, _) = match &config.storage.data_dir {
        Some(dir) => {
            let path = std::path::Path::new(dir);
            std::fs::create_dir_all(path).unwrap();
            let cat_path = path.join("catalog.json");
            let cat = Arc::new(nodus_catalog::MemoryCatalog::load_from_disk(cat_path).unwrap());
            let k = Arc::new(nodus_storage_lsm::LsmKvEngine::with_wal(path, encryption_key).unwrap());
            (k, cat)
        }
        None => {
            let cat = Arc::new(nodus_catalog::MemoryCatalog::new());
            let k = Arc::new(nodus_storage_mem::MemKvEngine::new());
            (k, cat)
        }
    };
    println!("Loaded KV and catalog");

    let cluster_version = catalog
        .get_cluster_version()
        .map(|v| v.active_version)
        .unwrap_or(1);
    let local_upgrade = Arc::new(nodus_upgrade::DefaultUpgradeCoordinator::new(
        1,
        vec!["new_storage_format".into()],
        cluster_version,
    ));

    println!("Initializing raft network and state");
    let raft_config = Arc::new(openraft::Config::default().validate().unwrap());
    let (log_store, state_machine) =
        openraft::storage::Adaptor::new(nodus_raftstore::NodusRaftStore::with_components(
            local_kv.clone(),
            catalog.clone(),
            catalog.clone(),
            local_upgrade.clone(),
        ));
    let raft_network = nodus_raftstore::network::NodusNetworkFactory::new("shard-meta".to_string());
    let raft = nodus_raftstore::server::NodusRaft::new(
        config.cluster.node_id,
        raft_config,
        raft_network,
        log_store,
        state_machine,
    )
    .await
    .map_err(|e| anyhow::anyhow!("raft init: {e}"))?;
    println!("Raft created");

    let raft_clone = raft.clone();
    let join_peers = config.cluster.join_peers.clone();
    let node_id = config.cluster.node_id;
    let advertise_addr = config.cluster.raft_advertise_addr.clone();
    let admin_token = config.admin.token.clone();

    let raft_kv = Arc::new(crate::raft_kv::RaftKvEngine {
        local: local_kv.clone(),
        raft: raft.clone(),
    });

    let raft_state = nodus_raftstore::server::RaftState::new();
    raft_state.rafts.write().await.insert("shard-meta".to_string(), raft.clone());

    let raft_catalog_writer = Arc::new(crate::raft_catalog::RaftCatalogWriter {
        local: catalog.clone(),
        reader: catalog.clone(),
        raft_state: raft_state.clone(),
    });

    let txn = Arc::new(nodus_txn::MemTxnManager::new());
    let authz = Arc::new(nodus_authz::DefaultAuthzEngine::new(catalog.clone()));

    let executor = Arc::new(nodus_executor::MemExecutor::new(
        catalog.clone(),
        raft_catalog_writer.clone(),
        authz.clone(),
        audit_sink,
        raft_kv,
        txn,
    ));

    println!("Executor created");

    let executor_clone = executor.clone();
    let state_clone = state.clone();
    tokio::spawn(async move {
        if join_peers.is_empty() {
            println!("Background initializing raft cluster as singleton");
            let mut nodes = std::collections::BTreeMap::new();
            nodes.insert(node_id, openraft::BasicNode::new(&advertise_addr));
            let _ = raft_clone.initialize(nodes).await;
            println!("Background raft initialization complete");
            
            // Wait for leader to establish
            let mut retries = 0;
            while retries < 10 {
                let leader = raft_clone.metrics().borrow().current_leader;
                println!("Waiting for leader... current_leader: {:?}", leader);
                if leader == Some(node_id) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                retries += 1;
            }
            
            // Bootstrap the catalog on the leader
            println!("Running bootstrap!");
            let db_id = nodus_catalog::DatabaseId::new();
            let cmd1 = nodus_raftstore::ShardCommand::CreateDatabase(nodus_catalog::CreateDatabaseRequest {
                id: db_id,
                name: "default".into(),
                owner_role_id: None,
            });
            if let Err(e) = raft_clone.client_write(cmd1).await {
                println!("Bootstrap create_database failed: {}", e);
            } else {
                let cmd2 = nodus_raftstore::ShardCommand::CreateSchema(nodus_catalog::CreateSchemaRequest {
                    id: nodus_catalog::SchemaId::new(),
                    database_id: db_id,
                    name: "public".into(),
                    owner_role_id: None,
                    managed_access: false,
                });
                if let Err(e) = raft_clone.client_write(cmd2).await {
                    println!("Bootstrap create_schema failed: {}", e);
                } else {
                    println!("Bootstrap succeeded");
                }
            }
            state_clone.is_ready.store(true, std::sync::atomic::Ordering::Release);
        } else {
            println!("Attempting to join existing cluster via {:?}", join_peers);
            let client = reqwest::Client::new();
            let mut joined = false;
            for _ in 0..5 {
                for peer in &join_peers {
                    let url = format!("http://{}/api/v1/cluster/join", peer);
                    let payload = serde_json::json!({
                        "node_id": node_id,
                        "raft_advertise_addr": advertise_addr
                    });
                    
                    let mut req = client.post(&url).json(&payload);
                    if let Some(token) = &admin_token {
                        req = req.bearer_auth(token);
                    }
                    
                    match req.send().await {
                        Ok(resp) if resp.status().is_success() => {
                            println!("Successfully joined cluster via {}", peer);
                            joined = true;
                            break;
                        }
                        Ok(resp) => {
                            let status = resp.status();
                            let text = resp.text().await.unwrap_or_default();
                            println!("Failed to join via {}: status {}, body: {}", peer, status, text);
                        }
                        Err(e) => {
                            println!("Failed to join via {}: {}", peer, e);
                        }
                    }
                }
                if joined {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
            if !joined {
                println!("FATAL: Failed to join cluster after retries");
            } else {
                state_clone.is_ready.store(true, std::sync::atomic::Ordering::Release);
            }
        }
    });

    let admin = match catalog.create_role(CreateRoleRequest {
        id: nodus_catalog::PrincipalId::new(),
        name: "nodus".into(),
        principal_type: PrincipalType::User,
        database_id: None,
    }) {
        Ok(desc) => desc,
        Err(e) if e.to_string().contains("already exists") => {
            catalog.get_principal_by_name("nodus")?
        }
        Err(e) => anyhow::bail!("seed admin: {e}"),
    };
    println!("Admin seeded");

    // Bootstrap superuser: ALL on System bypasses per-resource grant checks.
    let _ = catalog.grant_privilege(GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: admin.id,
        resource: ResourceRef::System,
        privilege: "ALL".into(),
    });
    // A read-only authz engine over the same catalog for the admin explain API.
    let authz = Arc::new(nodus_authz::DefaultAuthzEngine::new(catalog.clone()));
    let authenticator = Arc::new(PasswordAuthenticator::new(catalog.clone()));

    let admin_password = config.admin.password.clone().unwrap_or_else(|| {
        let generated = uuid::Uuid::new_v4().to_string();
        tracing::warn!(
            "No admin.password configured; generated random password for 'nodus' superuser: {}",
            generated
        );
        generated
    });
    authenticator.set_password("nodus", admin.id, &admin_password);

    let server_config = load_tls_config(&config.tls)?;
    let tls_acceptor = server_config.as_ref().map(|c| Arc::new(TlsAcceptor::from(c.clone())));
    println!("TLS acceptor loaded");

    // Background MVCC garbage collector: periodically reclaims superseded
    // versions below the transaction manager's safe watermark.
    let gc_executor = executor.clone();
    let gc_metrics = state.metrics.clone();
    let mut gc_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Ok(reclaimed) = gc_executor.run_gc()
                        && reclaimed > 0
                    {
                        gc_metrics.vacuum_reclaimed_total.inc_by(reclaimed as u64);
                    }
                }
                _ = gc_shutdown.changed() => {
                    break;
                }
            }
        }
    });

    // Slow-query log: queries slower than 200ms are retained (most recent 100).
    let slow_log = Arc::new(nodus_monitoring::SlowQueryLog::new(100, 200));

    let registry = Arc::new(SessionRegistry::new());
    let pgwire_metrics = state.metrics.clone();
    let pgwire_registry = registry.clone();
    let pgwire_slow_log = slow_log.clone();
    let pgwire_shutdown = shutdown.clone();
    let pgwire_registry = registry.clone();
    let pgwire_authenticator = authenticator.clone();
    let executor_pgwire = executor.clone();
    let pgwire_task = tokio::spawn(async move {
        nodus_pgwire::start_pgwire_server(
            pgwire_listener,
            executor_pgwire,
            pgwire_metrics,
            pgwire_registry,
            pgwire_authenticator,
            pgwire_slow_log,
            tls_acceptor,
            pgwire_shutdown,
        )
        .await
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let repo = Arc::new(FsBackupRepository::new(backup_dir(
        &config.backup.repository_uri,
    )));
    let backup = Arc::new(BackupOrchestrator::new(repo));

    // Background WAL archiver
    let backup_clone = backup.clone();
    let data_dir_clone = config.storage.data_dir.clone();
    let local_kv_clone = local_kv.clone();
    let mut wal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        if let Some(dir_path_str) = data_dir_clone {
            let dir_path = std::path::Path::new(&dir_path_str);
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let _ = local_kv_clone.flush();
                        if let Ok(entries) = std::fs::read_dir(dir_path) {
                            let mut logs = Vec::new();
                            for entry in entries.flatten() {
                                let path = entry.path();
                                if path.extension().and_then(|s| s.to_str()) == Some("log") {
                                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                                        if let Ok(id) = stem.parse::<u64>() {
                                            logs.push((id, path));
                                        }
                                    }
                                }
                            }
                            if !logs.is_empty() {
                                let max_id = logs.iter().map(|(id, _)| *id).max().unwrap();
                                for (id, path) in logs {
                                    if id < max_id {
                                        if let Ok(bytes) = std::fs::read(&path) {
                                            let filename = format!("{}.log", id);
                                            if backup_clone.archive_wal(&filename, bytes::Bytes::from(bytes)).await.is_ok() {
                                                let _ = std::fs::remove_file(&path);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    _ = wal_shutdown.changed() => {
                        break;
                    }
                }
            }
        }
    });

    let meta = Arc::new(nodus_meta::MemMetaStore::new());
    let shards = Arc::new(nodus_sharding::ShardOrchestrator::new(meta));

    let raft_upgrade_coordinator = Arc::new(crate::raft_upgrade::RaftUpgradeCoordinator {
        local: local_upgrade.clone(),
        raft_state: raft_state.clone(),
    });

    let admin_state = AdminState {
        registry: registry.clone(),
        audit: audit_query,
        authz: authz.clone(),
        catalog: catalog.clone(),
        catalog_writer: raft_catalog_writer.clone(),
        backup,
        upgrade: raft_upgrade_coordinator,
        shards,
        slow_log: slow_log.clone(),
        kv: executor.kv(),
        wal_key: encryption_key,
        draining: state.draining.clone(),
        authenticator: authenticator.clone(),
        admin_token: config.admin.token.clone(),
        raft_state: raft_state.clone(),
        membership_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    

    let app = Router::new()
        .merge(monitoring_routes(state.clone()))
        .merge(admin_routes(admin_state))
        .merge(nodus_web_console::web_console_routes())
        .merge(nodus_raftstore::server::raft_routes().with_state(raft_state))
        .layer(cors);

    let metrics_state = state.clone();
    let mut raft_metrics = raft.metrics();
    let mut rm_shutdown = shutdown.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                res = raft_metrics.changed() => {
                    if res.is_err() { break; }
                    let m = raft_metrics.borrow().clone();
                    let count = m.membership_config.membership().voter_ids().count() as u32;
                    metrics_state.cluster.nodes_total.store(count, std::sync::atomic::Ordering::Relaxed);
                    metrics_state.cluster.nodes_live.store(count, std::sync::atomic::Ordering::Relaxed); // Simplified: assuming all voters are live for MVP
                }
                _ = rm_shutdown.changed() => {
                    break;
                }
            }
        }
    });

    let qps_state = state.clone();
    let mut qps_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
        let mut last_queries = qps_state.metrics.queries_total.get();
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let current = qps_state.metrics.queries_total.get();
                    let delta = current.saturating_sub(last_queries);
                    last_queries = current;
                    qps_state.cluster.qps.store((delta as f64).to_bits(), std::sync::atomic::Ordering::Relaxed);
                }
                _ = qps_shutdown.changed() => {
                    break;
                }
            }
        }
    });

    let http_task = tokio::spawn(async move {
        if let Some(cfg) = server_config {
            let handle = axum_server::Handle::new();
            let handle_clone = handle.clone();
            tokio::spawn(async move {
                let _ = shutdown.changed().await;
                handle_clone.graceful_shutdown(Some(std::time::Duration::from_secs(5)));
            });
            let tls_config = axum_server::tls_rustls::RustlsConfig::from_config(cfg);
            axum_server::from_tcp_rustls(http_listener.into_std().unwrap(), tls_config).unwrap()
                .handle(handle)
                .serve(app.into_make_service())
                .await
        } else {
            axum::serve(http_listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown.changed().await;
                })
                .await
        }
    });

    Ok(ServerHandle {
        pgwire_addr,
        http_addr,
        pgwire_task,
        http_task,
        registry,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodus_config::TlsConfig;

    #[test]
    fn tls_disabled_yields_no_acceptor() {
        let cfg = TlsConfig::default();
        assert!(load_tls_config(&cfg).unwrap().is_none());
    }

    #[test]
    fn tls_enabled_without_paths_errors() {
        let cfg = TlsConfig {
            enabled: true,
            cert_path: None,
            key_path: None,
            client_ca_path: None,
            require_client_auth: false,
        };
        assert!(load_tls_config(&cfg).is_err());
    }

    #[test]
    fn backup_dir_parses_file_uri() {
        assert_eq!(
            backup_dir("file:///var/lib/nodus/backups"),
            PathBuf::from("/var/lib/nodus/backups")
        );
        // Empty falls back to a unique temp dir.
        assert!(backup_dir("").starts_with(std::env::temp_dir()));
    }
}
