mod admin;

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
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::RootCertStore;
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
fn load_tls_acceptor(tls: &nodus_config::TlsConfig) -> anyhow::Result<Option<Arc<TlsAcceptor>>> {
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
            WebPkiClientVerifier::builder(root_cert_store.into()).allow_unauthenticated().build()?
        }
    } else {
        WebPkiClientVerifier::no_client_auth()
    };

    let config = ServerConfig::builder()
        .with_client_cert_verifier(client_auth)
        .with_single_cert(certs, key)?;
    Ok(Some(Arc::new(TlsAcceptor::from(Arc::new(config)))))
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
        .store(true, std::sync::atomic::Ordering::Release);

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
        let bytes = hex::decode(hex_key).map_err(|e| anyhow::anyhow!("invalid encryption_key hex: {}", e))?;
        let mut key = [0u8; 32];
        if bytes.len() != 32 {
            return Err(anyhow::anyhow!("encryption_key must be exactly 32 bytes (64 hex characters)"));
        }
        key.copy_from_slice(&bytes);
        Some(key)
    } else {
        None
    };

    let (executor, catalog) = match &config.storage.data_dir {
        Some(dir) => nodus_executor::MemExecutor::persistent(audit_sink, dir, encryption_key)?,
        None => nodus_executor::MemExecutor::shared(audit_sink),
    };

    let admin = catalog
        .create_role(CreateRoleRequest {
            name: "nodus".into(),
            principal_type: PrincipalType::User,
            database_id: None,
        })
        .map_err(|e| anyhow::anyhow!("seed admin: {e}"))?;
    // Bootstrap superuser: ALL on System bypasses per-resource grant checks.
    catalog
        .grant_privilege(GrantPrivilegeRequest {
            principal_id: admin.id,
            resource: ResourceRef::System,
            privilege: "ALL".into(),
        })
        .map_err(|e| anyhow::anyhow!("seed admin grant: {e}"))?;
    // A read-only authz engine over the same catalog for the admin explain API.
    let authz = Arc::new(nodus_authz::DefaultAuthzEngine::new(catalog.clone()));
    let authenticator = Arc::new(PasswordAuthenticator::new(catalog.clone()));
    // Default development credentials; override before production use.
    authenticator.set_password("nodus", admin.id, "nodus");

    let tls_acceptor = load_tls_acceptor(&config.tls)?;

    // Background MVCC garbage collector: periodically reclaims superseded
    // versions below the transaction manager's safe watermark.
    let gc_executor = executor.clone();
    let gc_metrics = state.metrics.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            ticker.tick().await;
            if let Ok(reclaimed) = gc_executor.run_gc()
                && reclaimed > 0
            {
                gc_metrics.vacuum_reclaimed_total.inc_by(reclaimed as u64);
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
    let pgwire_task = tokio::spawn(async move {
        nodus_pgwire::start_pgwire_server(
            pgwire_listener,
            executor,
            pgwire_metrics,
            pgwire_registry,
            authenticator,
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

    let cluster_version = catalog
        .get_cluster_version()
        .map(|v| v.active_version)
        .unwrap_or(1);
    let upgrade = Arc::new(nodus_upgrade::DefaultUpgradeCoordinator::new(
        1,
        vec!["new_storage_format".into()],
        cluster_version,
    ));

    let meta = Arc::new(nodus_meta::MemMetaStore::new());
    let shards = Arc::new(nodus_sharding::ShardOrchestrator::new(meta));

    let admin_state = AdminState {
        registry: registry.clone(),
        audit: audit_query,
        authz: authz.clone(),
        catalog: catalog.clone(),
        backup,
        upgrade,
        shards,
        slow_log,
        draining: state.draining.clone(),
        admin_token: config.admin.token.clone(),
    };

    let raft_config = Arc::new(openraft::Config::default().validate().unwrap());
    let (log_store, state_machine) = openraft::storage::Adaptor::new(nodus_raftstore::NodusRaftStore::new());
    let raft_network = nodus_raftstore::network::NodusNetworkFactory::new();
    let raft = nodus_raftstore::server::NodusRaft::new(1, raft_config, raft_network, log_store, state_machine)
        .await
        .map_err(|e| anyhow::anyhow!("raft init: {e}"))?;
    let raft_state = nodus_raftstore::server::RaftState { raft };

    let app = Router::new()
        .merge(monitoring_routes(state))
        .merge(admin_routes(admin_state))
        .merge(nodus_web_console::web_console_routes())
        .merge(nodus_raftstore::server::raft_routes().with_state(raft_state))
        .layer(cors);

    let http_task = tokio::spawn(async move {
        axum::serve(http_listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown.changed().await;
            })
            .await
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
        assert!(load_tls_acceptor(&cfg).unwrap().is_none());
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
        assert!(load_tls_acceptor(&cfg).is_err());
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
