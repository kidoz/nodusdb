#![allow(clippy::collapsible_if)]
mod admin;
mod multi_raft;
mod raft_catalog;
mod raft_kv;
mod raft_router;
mod raft_shard_meta;
mod raft_upgrade;

use admin::{AdminState, admin_routes};
use axum::Router;
use nodus_backup::{BackupOrchestrator, FsBackupRepository, WalCommittedTxn};
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
    /// Detached-no-more background loops (MVCC GC, WAL archiver). They observe
    /// the shutdown watch and exit cleanly; retaining the handles lets the
    /// process await an in-flight pass instead of dropping it on the floor.
    pub background_tasks: Vec<JoinHandle<()>>,
    /// Shared registry of active client sessions (inspection + cancellation).
    pub registry: Arc<SessionRegistry>,
}

/// Reserved key under which the transaction clock's high-water mark is stored
/// in the durable local engine. The leading NUL keeps it out of the
/// `{table_id}:{pk}` row key space (and distinct from the catalog/raft keys).
const HLC_WATERMARK_KEY: &[u8] = b"\x00hlc\x00watermark";

/// Persists the transaction clock's high-water mark in the node-local durable
/// engine so commit timestamps never regress across a restart. Node-local on
/// purpose: each node is the timestamp authority for the writes it coordinates,
/// so this must not be replicated through Raft.
struct KvTimestampStore {
    kv: Arc<dyn nodus_storage_api::KvEngine>,
}

impl KvTimestampStore {
    fn new(kv: Arc<dyn nodus_storage_api::KvEngine>) -> Self {
        Self { kv }
    }
}

impl nodus_txn::TimestampStore for KvTimestampStore {
    fn load(&self) -> anyhow::Result<Option<u64>> {
        match self.kv.get(HLC_WATERMARK_KEY, u64::MAX)? {
            Some(bytes) if bytes.len() == 8 => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes);
                Ok(Some(u64::from_le_bytes(buf)))
            }
            _ => Ok(None),
        }
    }

    fn store(&self, watermark: u64) -> anyhow::Result<()> {
        // A single overwritten key; committing at `watermark` keeps versions
        // monotonic so a latest read always returns the newest reservation.
        let txn = nodus_storage_api::TxnId::new();
        self.kv.write_intent(
            txn,
            bytes::Bytes::from_static(HLC_WATERMARK_KEY),
            bytes::Bytes::copy_from_slice(&watermark.to_le_bytes()),
        )?;
        self.kv.commit(txn, watermark)?;
        Ok(())
    }
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

fn wal_archive_txn_index(
    bytes: &[u8],
    wal_key: Option<[u8; 32]>,
) -> anyhow::Result<(Vec<String>, Vec<WalCommittedTxn>, Option<u64>)> {
    use nodus_storage_wal::{FileWalEngine, WalEngine, WalRecord, WalRecordV1};

    let path = std::env::temp_dir().join(format!("nodus-wal-index-{}.log", uuid::Uuid::new_v4()));
    std::fs::write(&path, bytes)?;
    let result = (|| {
        let wal = FileWalEngine::with_encryption(&path, wal_key)?;
        let records = wal.recover()?;
        let mut record_txn_ids = Vec::new();
        let mut committed_txns = Vec::new();
        // The segment's lineage: the WAL-segment id this one follows (`None` for
        // the first/root segment, or a segment written before lineage records).
        let mut predecessor = None;
        for record in records {
            let WalRecord::V1(record) = record;
            match record {
                WalRecordV1::SegmentHeader { predecessor: prev } => {
                    predecessor = prev;
                }
                WalRecordV1::BeginTxn { txn_id }
                | WalRecordV1::WriteIntent { txn_id, .. }
                | WalRecordV1::DeleteIntent { txn_id, .. } => {
                    record_txn_ids.push(txn_id.0.to_string());
                }
                WalRecordV1::CommitTxn { txn_id, commit_ts } => {
                    committed_txns.push(WalCommittedTxn {
                        txn_id: txn_id.0.to_string(),
                        commit_ts,
                    });
                }
                WalRecordV1::AbortTxn { .. } | WalRecordV1::Checkpoint { .. } => {}
            }
        }
        Ok((record_txn_ids, committed_txns, predecessor))
    })();
    let _ = std::fs::remove_file(&path);
    result
}

/// Backoff for the cluster-join retry loop: exponential (250ms → cap 10s) with
/// a deterministic per-node jitter so simultaneously-starting joiners don't
/// retry in lockstep and hammer the seed node together.
fn join_backoff(attempt: u32, node_id: u64) -> std::time::Duration {
    use std::time::Duration;
    // 250ms * 2^attempt, saturating, capped at 10s. `attempt.min(6)` bounds the
    // shift so it can't overflow regardless of how long we've been retrying.
    let exp = Duration::from_millis(250).saturating_mul(1u32 << attempt.min(6));
    let capped = exp.min(Duration::from_secs(10));
    let jitter = Duration::from_millis(node_id.wrapping_mul(2_654_435_761) % 250);
    capped + jitter
}

fn bootstrap_catalog_commands(catalog: &dyn CatalogReader) -> Vec<nodus_raftstore::ShardCommand> {
    let mut commands = Vec::new();
    let db_id = match catalog.get_database("default") {
        Ok(database) => database.id,
        Err(_) => {
            let id = nodus_catalog::DatabaseId::new();
            commands.push(nodus_raftstore::ShardCommand::CreateDatabase(
                nodus_catalog::CreateDatabaseRequest {
                    id,
                    name: "default".into(),
                    owner_role_id: None,
                },
            ));
            id
        }
    };

    if catalog.get_schema("default", "public").is_err() {
        commands.push(nodus_raftstore::ShardCommand::CreateSchema(
            nodus_catalog::CreateSchemaRequest {
                id: nodus_catalog::SchemaId::new(),
                database_id: db_id,
                name: "public".into(),
                owner_role_id: None,
                managed_access: false,
            },
        ));
    }

    commands
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

/// Ensures a process-wide rustls crypto provider is installed (aws-lc-rs).
/// Idempotent: a no-op once one is present.
fn ensure_crypto_provider() {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// Builds the TLS server config for the dedicated Raft listener: presents this
/// node's certificate and **requires** every peer to present a client
/// certificate signed by the cluster CA. Returns `None` when peer TLS is
/// disabled. Mandatory client auth is safe here because this listener serves
/// only Raft RPCs, never admin/web traffic.
fn load_raft_tls_config(
    tls: &nodus_config::ClusterTlsConfig,
) -> anyhow::Result<Option<Arc<ServerConfig>>> {
    if !tls.enabled {
        return Ok(None);
    }
    let cert_path = tls
        .cert_path
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("cluster.tls.enabled but cert_path is unset"))?;
    let key_path = tls
        .key_path
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("cluster.tls.enabled but key_path is unset"))?;
    let ca_path = tls
        .ca_path
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("cluster.tls.enabled but ca_path is unset"))?;

    ensure_crypto_provider();

    let cert_bytes = std::fs::read(cert_path)?;
    let certs: Vec<CertificateDer> =
        rustls_pemfile::certs(&mut cert_bytes.as_slice()).collect::<Result<_, _>>()?;
    let key_bytes = std::fs::read(key_path)?;
    let key: PrivateKeyDer = rustls_pemfile::private_key(&mut key_bytes.as_slice())?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {key_path}"))?;

    let ca_bytes = std::fs::read(ca_path)?;
    let mut roots = RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut ca_bytes.as_slice()) {
        roots.add(cert?)?;
    }
    let client_auth = WebPkiClientVerifier::builder(roots.into()).build()?;

    let config = ServerConfig::builder()
        .with_client_cert_verifier(client_auth)
        .with_single_cert(certs, key)?;
    Ok(Some(Arc::new(config)))
}

/// Builds the outbound Raft RPC transport. With peer TLS disabled this is a
/// plain-HTTP client; with it enabled the client presents this node's
/// certificate and trusts only the cluster CA (so connections to peers are
/// mutually authenticated), and RPC URLs use `https`.
fn build_raft_transport(
    cluster: &nodus_config::ClusterConfig,
) -> anyhow::Result<nodus_raftstore::network::RaftTransport> {
    if !cluster.tls.enabled {
        return Ok(nodus_raftstore::network::RaftTransport::plain());
    }
    let cert_path = cluster
        .tls
        .cert_path
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("cluster.tls.enabled but cert_path is unset"))?;
    let key_path = cluster
        .tls
        .key_path
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("cluster.tls.enabled but key_path is unset"))?;
    let ca_path = cluster
        .tls
        .ca_path
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("cluster.tls.enabled but ca_path is unset"))?;

    ensure_crypto_provider();

    // `Identity::from_pem` scans the buffer for both the certificate chain and
    // the private key, so the two PEMs are simply concatenated.
    let mut identity_pem = std::fs::read(cert_path)?;
    identity_pem.push(b'\n');
    identity_pem.extend_from_slice(&std::fs::read(key_path)?);
    let identity = reqwest::Identity::from_pem(&identity_pem)?;
    let ca = reqwest::Certificate::from_pem(&std::fs::read(ca_path)?)?;

    let client = reqwest::Client::builder()
        .use_rustls_tls()
        // Trust only the cluster CA, not the system root store.
        .tls_built_in_root_certs(false)
        .add_root_certificate(ca)
        .identity(identity)
        .build()?;
    Ok(nodus_raftstore::network::RaftTransport::new(
        client, "https",
    ))
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
            let sink = Arc::new(nodus_audit::FileAuditSink::with_rotation(
                path,
                config.audit.max_size_bytes,
                config.audit.max_files,
            ));
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

    tracing::debug!("Loading KV and catalog");
    // Build the KV engine first, then back the catalog with that same store so
    // the catalog and user data share one durable mechanism and recovery path
    // (no separate catalog.json). The store wraps the local engine directly — a
    // materialization of the replicated catalog, not a routed write.
    let local_kv: Arc<dyn nodus_storage_api::KvEngine> = match &config.storage.data_dir {
        Some(dir) => {
            let path = std::path::Path::new(dir);
            std::fs::create_dir_all(path)
                .map_err(|e| anyhow::anyhow!("creating storage.data_dir '{dir}': {e}"))?;
            let engine = nodus_storage_lsm::LsmKvEngine::with_wal(path, encryption_key)
                .map_err(|e| anyhow::anyhow!("opening durable storage at '{dir}': {e}"))?;
            tracing::info!("Durable storage at {dir}");
            Arc::new(engine)
        }
        None => {
            // No data_dir => in-memory storage, which loses everything (Raft log,
            // catalog, data) on restart. Refuse unless explicitly allowed.
            if !config.storage.allow_ephemeral {
                anyhow::bail!(
                    "storage.data_dir is unset and storage.allow_ephemeral is false; \
                     set a data_dir for durable storage (or allow_ephemeral=true for dev)"
                );
            }
            tracing::warn!(
                "Running with IN-MEMORY storage (storage.data_dir is unset) — all data, \
                 including the Raft log, will be LOST on restart. Set storage.data_dir for durability."
            );
            Arc::new(nodus_storage_mem::MemKvEngine::new())
        }
    };
    let catalog = Arc::new(nodus_catalog::MemoryCatalog::with_store(Arc::new(
        nodus_executor::KvCatalogStore::new(local_kv.clone()),
    ))?);
    tracing::debug!("Loaded KV and catalog");

    let cluster_version = catalog
        .get_cluster_version()
        .map(|v| v.active_version)
        .unwrap_or(1);
    let local_upgrade = Arc::new(nodus_upgrade::DefaultUpgradeCoordinator::new(
        1,
        vec!["new_storage_format".into()],
        cluster_version,
    ));

    tracing::debug!("Initializing raft network and state");
    // Make snapshotting/compaction intentional rather than relying on
    // undocumented openraft defaults. `snapshot_max_chunk_size` bounds the
    // per-RPC payload so a large snapshot transfers as a series of chunks (each
    // `install_snapshot` request carries at most this many bytes) instead of one
    // oversized body; election/heartbeat timers stay at their defaults.
    let raft_config = Arc::new(
        openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(5000),
            snapshot_max_chunk_size: 4 * 1024 * 1024,
            max_in_snapshot_log_to_keep: 1000,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let raft_state = nodus_raftstore::server::RaftState::new();

    // Seed the transaction clock from a durable, node-local high-water mark so
    // commit timestamps never regress across a restart. Created before the Raft
    // manager so applied commits can advance it as groups ingest replicated writes.
    let txn = Arc::new(nodus_txn::MemTxnManager::with_timestamp_store(Arc::new(
        KvTimestampStore::new(local_kv.clone()),
    ))?);

    // Outbound Raft transport: plain HTTP, or an mTLS `https` client when
    // inter-node TLS is configured.
    let raft_transport = build_raft_transport(&config.cluster)?;

    // Owns this node's Raft groups. The meta group is created now; data-shard
    // groups are created on demand (routing lands in Phase 2).
    let multi_raft = Arc::new(crate::multi_raft::MultiRaftManager::new(
        config.cluster.node_id,
        config.cluster.raft_advertise_addr.clone(),
        raft_config,
        raft_state.clone(),
        local_kv.clone(),
        config.admin.token.clone(),
        txn.clone(),
        config
            .storage
            .data_dir
            .clone()
            .map(std::path::PathBuf::from),
        raft_transport,
    ));

    // Local cluster-metadata store (shard maps + placements), KV-backed so it
    // survives restart. The meta group's state machine applies shard-map and
    // placement commands into *this* store, so every node converges on it.
    let local_meta = Arc::new(nodus_meta::PersistentMetaStore::new(local_kv.clone()));

    let raft = multi_raft
        .create_meta(
            local_kv.clone(),
            catalog.clone(),
            catalog.clone(),
            local_upgrade.clone(),
            local_meta.clone(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("raft init: {e}"))?;
    tracing::debug!("Raft created");

    let raft_clone = raft.clone();
    let join_peers = config.cluster.join_peers.clone();
    let node_id = config.cluster.node_id;
    let advertise_addr = config.cluster.raft_advertise_addr.clone();
    let admin_token = config.admin.token.clone();
    let bootstrap_catalog = catalog.clone();

    // Async write-submission actor: bridges the synchronous KV/catalog write
    // traits to async Raft `client_write` without `block_in_place`.
    let raft_router = crate::raft_router::RaftRouter::spawn(multi_raft.clone());

    // Routing reads the LOCAL store (kept current by the meta group's apply
    // path), so every node routes identically.
    let shard_router: Arc<dyn nodus_sharding::ShardRouter> =
        Arc::new(nodus_sharding::CatalogShardRouter::new(local_meta.clone()));

    // The orchestrator WRITES shard maps/placements through the meta Raft group
    // (so all nodes apply them); reads fall through to the local store.
    let meta: Arc<dyn nodus_meta::MetaStore> =
        Arc::new(crate::raft_shard_meta::RaftShardMetaStore {
            local: local_meta.clone(),
            router: raft_router.clone(),
        });

    let raft_kv = Arc::new(crate::raft_kv::RaftKvEngine {
        local: local_kv.clone(),
        router: raft_router.clone(),
        shard_router: shard_router.clone(),
        manager: multi_raft.clone(),
        txn_groups: std::sync::Mutex::new(std::collections::HashMap::new()),
        metrics: state.metrics.clone(),
    });

    let raft_catalog_writer = Arc::new(crate::raft_catalog::RaftCatalogWriter {
        local: catalog.clone(),
        reader: catalog.clone(),
        router: raft_router.clone(),
        shard_id: crate::multi_raft::META_SHARD.to_string(),
    });

    let authz = Arc::new(nodus_authz::DefaultAuthzEngine::new(catalog.clone()));

    let executor = Arc::new(nodus_executor::MemExecutor::new(
        catalog.clone(),
        raft_catalog_writer.clone(),
        authz.clone(),
        audit_sink,
        raft_kv.clone(),
        txn,
    ));

    tracing::debug!("Executor created");

    let _executor_clone = executor.clone();
    let state_clone = state.clone();
    let recover_kv = raft_kv.clone();
    let reconcile_meta = meta.clone();
    let reconcile_manager = multi_raft.clone();
    tokio::spawn(async move {
        if join_peers.is_empty() {
            tracing::debug!("Background initializing raft cluster as singleton");
            let mut nodes = std::collections::BTreeMap::new();
            nodes.insert(node_id, openraft::BasicNode::new(&advertise_addr));
            let _ = raft_clone.initialize(nodes).await;
            tracing::debug!("Background raft initialization complete");

            // Wait for leader to establish
            let mut retries = 0;
            while retries < 10 {
                let leader = raft_clone.metrics().borrow().current_leader;
                tracing::debug!("Waiting for leader... current_leader: {:?}", leader);
                if leader == Some(node_id) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                retries += 1;
            }

            let bootstrap_commands = bootstrap_catalog_commands(bootstrap_catalog.as_ref());
            if bootstrap_commands.is_empty() {
                tracing::debug!("Bootstrap catalog already exists");
            } else {
                tracing::debug!(
                    "Running bootstrap with {} catalog command(s)",
                    bootstrap_commands.len()
                );
                for command in bootstrap_commands {
                    if let Err(e) = raft_clone.client_write(command).await {
                        tracing::debug!("Bootstrap catalog command failed: {}", e);
                        break;
                    }
                }
            }

            // Re-drive any cross-shard commit that was decided but not fully
            // applied before a restart. Runs on a blocking thread (it submits
            // through the Raft router) now that this node leads the meta group.
            let recover_kv = recover_kv.clone();
            match tokio::task::spawn_blocking(move || recover_kv.recover_pending_txns()).await {
                Ok(Ok(n)) if n > 0 => tracing::info!("Recovered {n} pending cross-shard commit(s)"),
                Ok(Ok(_)) => {}
                Ok(Err(e)) => tracing::warn!("2PC recovery failed: {e}"),
                Err(e) => tracing::warn!("2PC recovery task panicked: {e}"),
            }

            // Re-host data-shard groups assigned to this node. Placement is
            // durable (see `PersistentMetaStore`), so after a restart this
            // restores exactly the shards this node owned.
            match reconcile_meta.get_shard_placements() {
                Ok(placements) => match reconcile_manager.reconcile(&placements).await {
                    Ok(n) if n > 0 => {
                        tracing::info!("Reconciled {n} data-shard group(s) for this node")
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!("shard reconcile failed: {e}"),
                },
                Err(e) => tracing::warn!("could not load shard placements: {e}"),
            }

            state_clone
                .is_ready
                .store(true, std::sync::atomic::Ordering::Release);
        } else {
            tracing::debug!("Attempting to join existing cluster via {:?}", join_peers);
            let client = reqwest::Client::new();
            let mut joined = false;
            // A seed peer may still be bootstrapping (no meta group yet) or
            // mid-election (no leader yet) when we first try, and both answer
            // with a retryable status. Retry with exponential backoff over a
            // generous window rather than giving up after a fixed handful.
            const MAX_JOIN_ATTEMPTS: u32 = 30;
            let mut attempt = 0u32;
            while !joined && attempt < MAX_JOIN_ATTEMPTS {
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
                            tracing::debug!("Successfully joined cluster via {}", peer);
                            joined = true;
                            break;
                        }
                        Ok(resp) => {
                            let status = resp.status();
                            let text = resp.text().await.unwrap_or_default();
                            tracing::debug!(
                                "Join via {} not ready (status {}): {}",
                                peer,
                                status,
                                text
                            );
                        }
                        Err(e) => {
                            tracing::debug!("Failed to reach {} to join: {}", peer, e);
                        }
                    }
                }
                if joined {
                    break;
                }
                let backoff = join_backoff(attempt, node_id);
                tracing::debug!(
                    "Join attempt {} failed; retrying in {:?}",
                    attempt + 1,
                    backoff
                );
                tokio::time::sleep(backoff).await;
                attempt += 1;
            }
            if joined {
                state_clone
                    .is_ready
                    .store(true, std::sync::atomic::Ordering::Release);
            } else {
                tracing::warn!(
                    "Failed to join cluster via {:?} after {} attempts; node stays not-ready",
                    join_peers,
                    MAX_JOIN_ATTEMPTS
                );
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
    tracing::debug!("Admin seeded");

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
    let tls_acceptor = server_config
        .as_ref()
        .map(|c| Arc::new(TlsAcceptor::from(c.clone())));
    tracing::debug!("TLS acceptor loaded");

    let repo = Arc::new(FsBackupRepository::new(backup_dir(
        &config.backup.repository_uri,
    )));
    let backup = Arc::new(BackupOrchestrator::new(repo));

    // Graceful Raft shutdown: when the server is told to stop, shut down its
    // Raft groups so it stops participating in consensus (a node that keeps
    // heartbeating after "stopping" prevents its peers from re-electing).
    let raft_shutdown_manager = multi_raft.clone();
    let mut raft_shutdown = shutdown.clone();
    let raft_shutdown_task = tokio::spawn(async move {
        let _ = raft_shutdown.changed().await;
        raft_shutdown_manager.shutdown_all().await;
    });

    // Background MVCC garbage collector: periodically reclaims superseded
    // versions below the transaction manager's safe watermark, clamped by the
    // oldest retained backup snapshot.
    let gc_executor = executor.clone();
    let gc_backup = backup.clone();
    let gc_metrics = state.metrics.clone();
    let mut gc_shutdown = shutdown.clone();
    let gc_task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let protected = gc_backup.protected_gc_watermark().await.ok().flatten();
                    if let Ok(reclaimed) = gc_executor.run_gc_with_protected_watermark(protected)
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
    let pgwire_slow_log = slow_log.clone();
    let pgwire_shutdown = shutdown.clone();
    let pgwire_registry = registry.clone();
    let pgwire_authenticator = authenticator.clone();
    let executor_pgwire = executor.clone();
    let pgwire_max_connections = config.server.max_connections as usize;
    let pgwire_task = tokio::spawn(async move {
        nodus_pgwire::start_pgwire_server(
            pgwire_listener,
            executor_pgwire,
            pgwire_metrics,
            pgwire_registry,
            pgwire_authenticator,
            pgwire_slow_log,
            tls_acceptor,
            pgwire_max_connections,
            pgwire_shutdown,
        )
        .await
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Background WAL archiver
    let backup_clone = backup.clone();
    let data_dir_clone = config.storage.data_dir.clone();
    let local_kv_clone = local_kv.clone();
    let wal_key = encryption_key;
    let mut wal_shutdown = shutdown.clone();
    let wal_archiver_task = tokio::spawn(async move {
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
                                            if let Ok((
                                                record_txn_ids,
                                                committed_txns,
                                                predecessor,
                                            )) = wal_archive_txn_index(&bytes, wal_key)
                                            {
                                                let data = bytes::Bytes::from(bytes);
                                                if backup_clone
                                                    .archive_wal_indexed(
                                                        &filename,
                                                        data,
                                                        record_txn_ids,
                                                        committed_txns,
                                                        predecessor,
                                                    )
                                                    .await
                                                    .is_ok()
                                                    && backup_clone
                                                        .wal_segment_cleanup_allowed(&filename)
                                                        .await
                                                        .unwrap_or(false)
                                                {
                                                    let _ = std::fs::remove_file(&path);
                                                }
                                            } else {
                                                let _ = backup_clone
                                                    .archive_wal(
                                                        &filename,
                                                        bytes::Bytes::from(bytes),
                                                    )
                                                    .await;
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

    let shards = Arc::new(nodus_sharding::ShardOrchestrator::new(meta.clone()));

    let raft_upgrade_coordinator = Arc::new(crate::raft_upgrade::RaftUpgradeCoordinator {
        local: local_upgrade.clone(),
        router: raft_router.clone(),
        shard_id: crate::multi_raft::META_SHARD.to_string(),
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
        manager: multi_raft.clone(),
        slow_log: slow_log.clone(),
        kv: executor.kv(),
        executor: executor.clone(),
        wal_key: encryption_key,
        draining: state.draining.clone(),
        authenticator: authenticator.clone(),
        admin_token: config.admin.token.clone(),
        raft_state: raft_state.clone(),
        membership_lock: Arc::new(tokio::sync::Mutex::new(())),
        restore_lock: Arc::new(tokio::sync::Mutex::new(())),
        restoring: executor.restoring_flag(),
        restore_gate: executor.restore_gate(),
    };

    // Raft RPCs run on a dedicated, mTLS-capable listener when
    // `cluster.raft_listen_addr` is set; otherwise they share the public HTTP
    // listener (the historical layout, used by single-port/dev deployments).
    let raft_listen_addr = config.cluster.raft_listen_addr.clone();
    let mut app = Router::new()
        .merge(monitoring_routes(state.clone()))
        .merge(admin_routes(admin_state))
        .merge(nodus_web_console::web_console_routes());
    if raft_listen_addr.is_none() {
        app = app.merge(nodus_raftstore::server::raft_routes().with_state(raft_state.clone()));
    }
    let app = app.layer(cors);

    // Dedicated Raft peer listener (separate from admin/web), with mandatory
    // client-certificate mTLS when `cluster.tls` is enabled.
    let raft_serve_task = if let Some(addr) = raft_listen_addr.clone() {
        let raft_server_config = load_raft_tls_config(&config.cluster.tls)?;
        let raft_listener = TcpListener::bind(&addr).await?;
        let raft_app =
            Router::new().merge(nodus_raftstore::server::raft_routes().with_state(raft_state));
        let mut raft_shutdown = shutdown.clone();
        tracing::info!(
            "Raft peer listener on {} (mTLS: {})",
            addr,
            raft_server_config.is_some()
        );
        Some(tokio::spawn(async move {
            if let Some(cfg) = raft_server_config {
                let handle = axum_server::Handle::new();
                let handle_clone = handle.clone();
                tokio::spawn(async move {
                    let _ = raft_shutdown.changed().await;
                    handle_clone.graceful_shutdown(Some(std::time::Duration::from_secs(5)));
                });
                let tls_config = axum_server::tls_rustls::RustlsConfig::from_config(cfg);
                let _ = axum_server::from_tcp_rustls(raft_listener.into_std().unwrap(), tls_config)
                    .unwrap()
                    .handle(handle)
                    .serve(raft_app.into_make_service())
                    .await;
            } else {
                let _ = axum::serve(raft_listener, raft_app)
                    .with_graceful_shutdown(async move {
                        let _ = raft_shutdown.changed().await;
                    })
                    .await;
            }
        }))
    } else {
        None
    };

    let metrics_state = state.clone();
    let metrics_manager = multi_raft.clone();
    let metrics_node_id = config.cluster.node_id;
    let mut raft_metrics = raft.metrics();
    let mut rm_shutdown = shutdown.clone();
    tokio::spawn(async move {
        use std::sync::atomic::Ordering::Relaxed;
        let mut shard_tick = tokio::time::interval(std::time::Duration::from_secs(2));
        loop {
            tokio::select! {
                res = raft_metrics.changed() => {
                    if res.is_err() { break; }
                    let m = raft_metrics.borrow().clone();
                    let voters = m.membership_config.membership().voter_ids().count() as u32;
                    // Live nodes from the meta leader's view: itself, plus voters
                    // it is actively replicating to. A follower cannot assess its
                    // peers, so it reports the full membership. (Heartbeat-recency
                    // liveness would be more precise; this replaces the previous
                    // "assume every voter is live".)
                    let live = if m.current_leader == Some(metrics_node_id) {
                        let followers = m
                            .replication
                            .as_ref()
                            .map(|r| r.values().filter(|v| v.is_some()).count() as u32)
                            .unwrap_or(0);
                        (1 + followers).min(voters.max(1))
                    } else {
                        voters
                    };
                    metrics_state.cluster.nodes_total.store(voters, Relaxed);
                    metrics_state.cluster.nodes_live.store(live, Relaxed);
                }
                _ = shard_tick.tick() => {
                    let (total, unavailable) = metrics_manager.shard_health().await;
                    metrics_state.cluster.shards_total.store(total, Relaxed);
                    metrics_state.cluster.shards_unavailable.store(unavailable, Relaxed);
                    metrics_state.metrics.shard_groups.set(total as i64);
                    metrics_state.metrics.shard_groups_unavailable.set(unavailable as i64);
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

    // Periodically reconcile data-shard groups against current placements:
    // forms multi-node groups for shards owned here, folds in nodes that joined
    // after a group formed, and re-instantiates replicas on restarted peers.
    // Convergent and idempotent, so a missed/raced admin-triggered reconcile is
    // eventually repaired.
    let reconcile_loop_meta = meta.clone();
    let reconcile_loop_manager = multi_raft.clone();
    let mut reconcile_shutdown = shutdown.clone();
    let shard_reconcile_task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(10));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Ok(placements) = reconcile_loop_meta.get_shard_placements() {
                        if let Err(e) = reconcile_loop_manager.reconcile(&placements).await {
                            tracing::debug!("periodic shard reconcile: {e}");
                        }
                    }
                }
                _ = reconcile_shutdown.changed() => break,
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
            axum_server::from_tcp_rustls(http_listener.into_std().unwrap(), tls_config)
                .unwrap()
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
        background_tasks: {
            let mut tasks = vec![
                gc_task,
                wal_archiver_task,
                raft_shutdown_task,
                shard_reconcile_task,
            ];
            if let Some(task) = raft_serve_task {
                tasks.push(task);
            }
            tasks
        },
        registry,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodus_catalog::MemoryCatalog;
    use nodus_config::TlsConfig;

    #[test]
    fn tls_disabled_yields_no_acceptor() {
        let cfg = TlsConfig::default();
        assert!(load_tls_config(&cfg).unwrap().is_none());
    }

    #[test]
    fn raft_transport_is_plain_when_cluster_tls_disabled() {
        let cluster = nodus_config::ClusterConfig::default();
        let transport = build_raft_transport(&cluster).unwrap();
        assert_eq!(transport.scheme(), "http");
    }

    /// Builds a tiny PKI — a CA plus one leaf certificate signed by it (valid for
    /// both server and client auth, with a `127.0.0.1` IP SAN) — and writes the
    /// leaf cert, leaf key, and CA cert PEMs into `dir`. The one leaf doubles as
    /// every node's identity, which is all a one-host mTLS handshake test needs.
    fn write_test_certs(dir: &std::path::Path) -> (String, String, String) {
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let mut leaf_params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        leaf_params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

        let cert_path = dir.join("node.crt");
        let key_path = dir.join("node.key");
        let ca_path = dir.join("ca.crt");
        std::fs::write(&cert_path, leaf_cert.pem()).unwrap();
        std::fs::write(&key_path, leaf_key.serialize_pem()).unwrap();
        std::fs::write(&ca_path, ca_cert.pem()).unwrap();
        (
            cert_path.to_string_lossy().into_owned(),
            key_path.to_string_lossy().into_owned(),
            ca_path.to_string_lossy().into_owned(),
        )
    }

    /// The dedicated Raft listener with mandatory mTLS accepts a peer presenting a
    /// CA-signed client certificate and rejects one that presents none.
    #[tokio::test]
    async fn raft_listener_enforces_mutual_tls() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path, ca_path) = write_test_certs(dir.path());

        let cluster = nodus_config::ClusterConfig {
            tls: nodus_config::ClusterTlsConfig {
                enabled: true,
                cert_path: Some(cert_path.clone()),
                key_path: Some(key_path.clone()),
                ca_path: Some(ca_path.clone()),
            },
            raft_listen_addr: Some("127.0.0.1:0".into()),
            ..Default::default()
        };

        // Server: serve the Raft routes with mandatory client-cert mTLS.
        let server_cfg = load_raft_tls_config(&cluster.tls).unwrap().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().merge(
            nodus_raftstore::server::raft_routes()
                .with_state(nodus_raftstore::server::RaftState::new()),
        );
        let tls_config = axum_server::tls_rustls::RustlsConfig::from_config(server_cfg);
        tokio::spawn(async move {
            let _ = axum_server::from_tcp_rustls(listener.into_std().unwrap(), tls_config)
                .unwrap()
                .serve(app.into_make_service())
                .await;
        });
        // Give the listener a moment to start accepting.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let url = format!("https://127.0.0.1:{}/raft/shard-meta/vote", addr.port());
        let body = serde_json::json!({
            "vote": {"leader_id": {"term": 1, "node_id": 1}, "committed": false},
            "last_log_id": null
        });

        // A peer presenting the cluster-signed identity completes the handshake;
        // the unknown shard then yields a clean 404 (proving TLS succeeded).
        let mut identity_pem = std::fs::read(&cert_path).unwrap();
        identity_pem.push(b'\n');
        identity_pem.extend_from_slice(&std::fs::read(&key_path).unwrap());
        let ca = reqwest::Certificate::from_pem(&std::fs::read(&ca_path).unwrap()).unwrap();
        let mtls_client = reqwest::Client::builder()
            .use_rustls_tls()
            .tls_built_in_root_certs(false)
            .add_root_certificate(ca.clone())
            .identity(reqwest::Identity::from_pem(&identity_pem).unwrap())
            .build()
            .unwrap();
        let resp = mtls_client.post(&url).json(&body).send().await;
        let status = resp.expect("mTLS peer should connect").status();
        assert_eq!(status, reqwest::StatusCode::NOT_FOUND);

        // A client with no certificate is rejected at the TLS layer.
        let no_cert_client = reqwest::Client::builder()
            .use_rustls_tls()
            .tls_built_in_root_certs(false)
            .add_root_certificate(ca)
            .build()
            .unwrap();
        assert!(
            no_cert_client.post(&url).json(&body).send().await.is_err(),
            "a peer without a client certificate must be rejected"
        );
    }

    #[tokio::test]
    async fn refuses_to_start_in_memory_when_ephemeral_disallowed() {
        let pgwire = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

        // data_dir unset + allow_ephemeral=false must fail fast rather than
        // silently run non-durable.
        let mut config = NodusConfig::default();
        config.storage.allow_ephemeral = false;

        let result = run_server_with_config(pgwire, http, config, shutdown_rx).await;
        assert!(
            result.is_err(),
            "server must refuse in-memory storage when allow_ephemeral is false"
        );
    }

    #[test]
    fn join_backoff_grows_then_caps() {
        // Monotonic non-decreasing and capped at 10s + max jitter (<250ms).
        let ceiling = std::time::Duration::from_secs(10) + std::time::Duration::from_millis(250);
        let mut prev = std::time::Duration::ZERO;
        for attempt in 0..12 {
            let d = join_backoff(attempt, 1);
            assert!(d >= prev, "backoff must not shrink at attempt {attempt}");
            assert!(
                d <= ceiling,
                "backoff must stay capped at attempt {attempt}"
            );
            prev = d;
        }
        // Reaches the cap and stays there for large attempts.
        assert!(join_backoff(20, 1) <= ceiling);
        assert!(join_backoff(20, 1) >= std::time::Duration::from_secs(10));
    }

    #[test]
    fn join_backoff_jitter_desyncs_nodes() {
        // Different node ids get different jitter at the same attempt, so
        // simultaneous joiners don't retry in lockstep.
        let a = join_backoff(3, 1);
        let b = join_backoff(3, 2);
        assert_ne!(a, b);
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

    #[test]
    fn wal_archive_txn_index_reads_real_wal_bytes() {
        use nodus_storage_api::TxnId;
        use nodus_storage_wal::{FileWalEngine, WalEngine, WalRecord, WalRecordV1};

        let path =
            std::env::temp_dir().join(format!("nodus-wal-test-{}.log", uuid::Uuid::new_v4()));
        let wal = FileWalEngine::new(&path).unwrap();
        let txn_id = TxnId::new();
        wal.append(WalRecord::V1(WalRecordV1::BeginTxn { txn_id }))
            .unwrap();
        wal.append(WalRecord::V1(WalRecordV1::WriteIntent {
            txn_id,
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        }))
        .unwrap();
        wal.append(WalRecord::V1(WalRecordV1::CommitTxn {
            txn_id,
            commit_ts: 42,
        }))
        .unwrap();
        wal.sync().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        let (record_txn_ids, committed_txns, _predecessor) =
            wal_archive_txn_index(&bytes, None).unwrap();
        let _ = std::fs::remove_file(&path);

        assert!(record_txn_ids.contains(&txn_id.0.to_string()));
        assert_eq!(committed_txns.len(), 1);
        assert_eq!(committed_txns[0].txn_id, txn_id.0.to_string());
        assert_eq!(committed_txns[0].commit_ts, 42);
    }

    #[test]
    fn bootstrap_catalog_commands_create_missing_database_and_schema() {
        let catalog = MemoryCatalog::new();
        let commands = bootstrap_catalog_commands(&catalog);

        assert_eq!(commands.len(), 2);
        let db_id = match &commands[0] {
            nodus_raftstore::ShardCommand::CreateDatabase(req) => {
                assert_eq!(req.name, "default");
                req.id
            }
            other => panic!("expected CreateDatabase, got {other:?}"),
        };
        match &commands[1] {
            nodus_raftstore::ShardCommand::CreateSchema(req) => {
                assert_eq!(req.name, "public");
                assert_eq!(req.database_id, db_id);
            }
            other => panic!("expected CreateSchema, got {other:?}"),
        }
    }

    #[test]
    fn bootstrap_catalog_commands_skip_existing_database_and_schema() {
        let catalog = MemoryCatalog::new();
        let db = catalog
            .create_database(nodus_catalog::CreateDatabaseRequest {
                id: nodus_catalog::DatabaseId::new(),
                name: "default".into(),
                owner_role_id: None,
            })
            .unwrap();
        catalog
            .create_schema(nodus_catalog::CreateSchemaRequest {
                id: nodus_catalog::SchemaId::new(),
                database_id: db.id,
                name: "public".into(),
                owner_role_id: None,
                managed_access: false,
            })
            .unwrap();

        assert!(bootstrap_catalog_commands(&catalog).is_empty());
    }

    #[test]
    fn bootstrap_catalog_commands_reuse_existing_database_for_missing_schema() {
        let catalog = MemoryCatalog::new();
        let db = catalog
            .create_database(nodus_catalog::CreateDatabaseRequest {
                id: nodus_catalog::DatabaseId::new(),
                name: "default".into(),
                owner_role_id: None,
            })
            .unwrap();

        let commands = bootstrap_catalog_commands(&catalog);
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            nodus_raftstore::ShardCommand::CreateSchema(req) => {
                assert_eq!(req.name, "public");
                assert_eq!(req.database_id, db.id);
            }
            other => panic!("expected CreateSchema, got {other:?}"),
        }
    }
}
