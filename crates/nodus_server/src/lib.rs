#![allow(clippy::collapsible_if)]
mod admin;
pub mod migration_coordinator;
#[cfg(test)]
mod migration_tests;
mod multi_raft;
mod raft_catalog;
mod raft_kv;
mod raft_router;
mod raft_shard_meta;
mod raft_upgrade;
#[cfg(test)]
mod snapshot_auth_tests;
#[cfg(test)]
mod snapshot_tests;
mod tls;
#[cfg(test)]
mod upgrade_tests;

use admin::{AdminState, admin_routes};
use axum::Router;
use nodus_backup::{
    BackupOrchestrator, BackupRepository, FsBackupRepository, S3BackupRepository, WalCommittedTxn,
};
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
use tls::ReloadableCertResolver;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::RootCertStore;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tower_http::cors::{Any, CorsLayer};

pub struct ServerHandle {
    #[cfg(any(test, feature = "test-utils"))]
    catalog: Arc<nodus_catalog::MemoryCatalog>,
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

#[cfg(any(test, feature = "test-utils"))]
impl ServerHandle {
    pub fn catalog(&self) -> Arc<nodus_catalog::MemoryCatalog> {
        self.catalog.clone()
    }
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

/// The backup repository selected by `backup.repository_uri`.
#[derive(Debug, PartialEq, Eq)]
enum BackupRepositoryKind {
    /// Local filesystem directory (`file://…` or a bare path; empty falls back
    /// to a unique temp directory so the backup API stays usable in dev/tests).
    Fs(PathBuf),
    /// S3-compatible object store (`s3://bucket[/prefix]`). Credentials,
    /// region, and endpoint come from the standard AWS-compatible environment
    /// variables, never from the config file.
    S3 { bucket: String, prefix: String },
}

fn parse_backup_repository_uri(uri: &str) -> anyhow::Result<BackupRepositoryKind> {
    if let Some(rest) = uri.strip_prefix("s3://") {
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            anyhow::bail!(
                "backup.repository_uri {uri:?} must name a bucket: s3://<bucket>[/<prefix>]"
            );
        }
        return Ok(BackupRepositoryKind::S3 {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
        });
    }
    if let Some((scheme, _)) = uri.split_once("://")
        && scheme != "file"
    {
        anyhow::bail!(
            "backup.repository_uri {uri:?} uses unsupported scheme {scheme:?}; expected file:// or s3://"
        );
    }
    Ok(BackupRepositoryKind::Fs(backup_dir(uri)))
}

/// Builds the backup repository for `backup.repository_uri`: filesystem for
/// `file://`/bare paths, S3-compatible object store for `s3://`. Unknown
/// schemes fail startup with an actionable error rather than silently becoming
/// an unintended local directory name.
fn backup_repository(uri: &str) -> anyhow::Result<Arc<dyn BackupRepository>> {
    match parse_backup_repository_uri(uri)? {
        BackupRepositoryKind::Fs(dir) => Ok(Arc::new(FsBackupRepository::new(dir))),
        BackupRepositoryKind::S3 { bucket, prefix } => {
            let repo = S3BackupRepository::from_env(&bucket, &prefix).map_err(|error| {
                anyhow::anyhow!(
                    "backup.repository_uri {uri:?}: {error}; S3 credentials, region, and \
                     endpoint come from the AWS-compatible environment (AWS_ACCESS_KEY_ID, \
                     AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ENDPOINT_URL, AWS_ALLOW_HTTP)"
                )
            })?;
            tracing::info!(
                bucket,
                prefix,
                "backup repository: S3-compatible object store"
            );
            Ok(Arc::new(repo))
        }
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

async fn archive_closed_wal_segments(
    dir_path: &std::path::Path,
    backup: &BackupOrchestrator,
    local_kv: &Arc<dyn nodus_storage_api::KvEngine>,
    wal_key: Option<[u8; 32]>,
) {
    let _ = local_kv.flush();
    let Ok(entries) = std::fs::read_dir(dir_path) else {
        return;
    };
    let mut logs = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("log")
            && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
            && let Ok(id) = stem.parse::<u64>()
        {
            logs.push((id, path));
        }
    }
    logs.sort_by_key(|(id, _)| *id);
    let Some(max_id) = logs.iter().map(|(id, _)| *id).max() else {
        return;
    };

    let mut archived = Vec::new();
    for (id, path) in logs {
        if id >= max_id {
            continue;
        }
        if let Ok(bytes) = std::fs::read(&path) {
            let filename = format!("{}.log", id);
            if let Ok((record_txn_ids, committed_txns, predecessor)) =
                wal_archive_txn_index(&bytes, wal_key)
            {
                if backup
                    .archive_wal_indexed(
                        &filename,
                        bytes::Bytes::from(bytes),
                        record_txn_ids,
                        committed_txns,
                        predecessor,
                    )
                    .await
                    .is_ok()
                {
                    archived.push((id, filename));
                }
            } else {
                let _ = backup
                    .archive_wal(&filename, bytes::Bytes::from(bytes))
                    .await;
            }
        }
    }

    if archived.is_empty() {
        return;
    }
    let retention = match backup.retention_view().await {
        Ok(view) => Some(view),
        Err(error) => {
            tracing::warn!(%error, "backup repository scan failed; WAL cleanup denied this pass");
            None
        }
    };
    for (id, filename) in archived {
        let cleanup_allowed = match &retention {
            Some(view) => view
                .wal_segment_cleanup_allowed(&filename)
                .await
                .unwrap_or(false),
            None => false,
        };
        if cleanup_allowed && let Err(error) = local_kv.reclaim_archived_wal_segment(id) {
            tracing::warn!(segment_id = id, %error, "archived WAL cleanup failed; retaining local segment");
        }
    }
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

/// Builds the pgwire/HTTP TLS config from configuration. Returns `None` when TLS
/// is disabled. The server certificate is served through a reloadable resolver
/// (returned alongside the config) so it can be rotated on `SIGHUP`. Errors if
/// enabled but the cert/key are missing or invalid.
fn load_tls_config(
    tls: &nodus_config::TlsConfig,
) -> anyhow::Result<Option<(Arc<ServerConfig>, Arc<ReloadableCertResolver>)>> {
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

    ensure_crypto_provider();

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

    let resolver = ReloadableCertResolver::load(cert_path, key_path)?;
    let config = ServerConfig::builder()
        .with_client_cert_verifier(client_auth)
        .with_cert_resolver(resolver.clone());
    Ok(Some((Arc::new(config), resolver)))
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
) -> anyhow::Result<Option<(Arc<ServerConfig>, Arc<ReloadableCertResolver>)>> {
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

    let ca_bytes = std::fs::read(ca_path)?;
    let mut roots = RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut ca_bytes.as_slice()) {
        roots.add(cert?)?;
    }
    let client_auth = WebPkiClientVerifier::builder(roots.into()).build()?;

    let resolver = ReloadableCertResolver::load(cert_path, key_path)?;
    let config = ServerConfig::builder()
        .with_client_cert_verifier(client_auth)
        .with_cert_resolver(resolver.clone());
    Ok(Some((Arc::new(config), resolver)))
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
        // Trust only the cluster CA, not the system root store. reqwest 0.13's
        // `tls_certs_only` both disables the built-in roots and adds our CA.
        .tls_certs_only([ca])
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .identity(identity)
        .build()?;
    Ok(nodus_raftstore::network::RaftTransport::new(
        client, "https",
    ))
}

/// The auth policy for Raft HTTP routes when they are reachable without
/// transport-level peer authentication (the shared public listener, or a
/// dedicated listener without `cluster.tls`): the admin bearer token, with the
/// same explicit `allow_insecure` opt-out as the admin API.
fn raft_route_auth(config: &NodusConfig) -> nodus_raftstore::server::RaftRouteAuth {
    nodus_raftstore::server::RaftRouteAuth::AdminToken {
        token: config.admin.token.clone().map(Into::into),
        allow_insecure: config.admin.allow_insecure,
    }
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
    shutdown: tokio::sync::watch::Receiver<()>,
) -> anyhow::Result<ServerHandle> {
    run_server_full(pgwire_listener, http_listener, None, config, shutdown).await
}

/// Full startup: optionally takes a prebound Raft peer listener so callers can
/// pin its port without a bind race (the address must match
/// `cluster.raft_listen_addr`, which is what peers dial).
pub async fn run_server_full(
    pgwire_listener: TcpListener,
    http_listener: TcpListener,
    raft_listener: Option<TcpListener>,
    config: NodusConfig,
    mut shutdown: tokio::sync::watch::Receiver<()>,
) -> anyhow::Result<ServerHandle> {
    let pgwire_addr = pgwire_listener.local_addr()?;
    let http_addr = http_listener.local_addr()?;
    // Sessions report the server's own port and connection limit.
    nodus_executor::set_server_setting("port", &pgwire_addr.port().to_string());
    nodus_executor::set_server_setting(
        "max_connections",
        &config.server.max_connections.to_string(),
    );

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

    tracing::debug!("Initializing raft network and state");
    // Make snapshotting/compaction intentional rather than relying on
    // undocumented openraft defaults. `snapshot_max_chunk_size` bounds the
    // per-RPC payload so a large snapshot transfers as a series of chunks (each
    // `install_snapshot` request carries at most this many bytes) instead of one
    // oversized body.
    //
    // Heartbeat/election timers come from cluster config (see [`ClusterConfig`]),
    // defaulting well past openraft's own 50ms/150-300ms — which assume a cheap
    // in-process or UDP transport. NodusDB replicates over HTTP+JSON (reqwest),
    // and openraft bounds each AppendEntries by exactly `heartbeat_interval` (see
    // replication: `C::timeout(heartbeat, append_entries)`). At 50ms a cold
    // connection — worse under mTLS — plus the follower's real WAL append
    // routinely overran the deadline, so heartbeats timed out and elections fired
    // spuriously, churning leadership. The default heartbeat gives each RPC ample
    // room (warm-pool reuse keeps it far under that), and the election window
    // only replaces a leader after several genuinely missed beats. Tests override
    // these with openraft's fast defaults, since loopback has no such latency.
    let raft_config = Arc::new(
        openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(5000),
            snapshot_max_chunk_size: 4 * 1024 * 1024,
            max_in_snapshot_log_to_keep: 1000,
            heartbeat_interval: config.cluster.raft_heartbeat_ms,
            election_timeout_min: config.cluster.raft_election_timeout_min_ms,
            election_timeout_max: config.cluster.raft_election_timeout_max_ms,
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
    // inter-node TLS is configured. The admin token is attached as a bearer
    // credential so peers that serve Raft RPCs on their public listener accept
    // them (mTLS peer listeners authenticate via the client certificate).
    let raft_transport = build_raft_transport(&config.cluster)?
        .with_snapshot_checks()
        .with_admission_source(local_kv.clone())
        .with_auth_token(config.admin.token.clone());
    let authority = nodus_raftstore::upgrade::read(local_kv.as_ref())?;
    let admissions = nodus_raftstore::upgrade::admission::read(local_kv.as_ref())?;
    nodus_raftstore::upgrade::admission::approved(&authority, admissions.as_ref())?;

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
        raft_transport.clone(),
    ));
    // Peer-driven replica formation works over the raft listener (including
    // mTLS-only deployments that do not serve the admin API there).
    multi_raft.install_instantiate_hook();

    // Local cluster-metadata store (shard maps + placements), KV-backed so it
    // survives restart. The meta group's state machine applies shard-map and
    // placement commands into *this* store, so every node converges on it.
    let local_meta = Arc::new(nodus_meta::PersistentMetaStore::new(local_kv.clone()));

    let raft = multi_raft
        .create_meta(
            local_kv.clone(),
            catalog.clone(),
            catalog.clone(),
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
    // traits to async Raft `client_write` without `block_in_place`. Migration
    // protocol commands unlock only once this node's durable upgrade authority
    // has finalized the migration cluster level; the gate fails closed on any
    // read error.
    let gate_kv = local_kv.clone();
    let raft_router = crate::raft_router::RaftRouter::spawn(multi_raft.clone())
        .with_migration_gate(move || {
            nodus_raftstore::upgrade::read(gate_kv.as_ref()).is_ok_and(|authority| {
                authority.cluster_version >= nodus_raftstore::upgrade::MIGRATION_LEVEL
            })
        });

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
        reader: catalog.clone(),
        router: raft_router.clone(),
        shard_id: crate::multi_raft::META_SHARD.to_string(),
    });

    // 2PC recovery runs on ANY meta-group leader — on leadership gain and
    // periodically while leader — not just in the bootstrap branch: if the
    // node that recorded a commit decision dies permanently, a survivor that
    // joined via join_peers re-drives the decided commit (see
    // `raft_kv::pending_txn_recovery_loop`).
    let recovery_raft = raft.clone();
    let recovery_kv = raft_kv.clone();
    let recovery_shutdown = shutdown.clone();
    let recovery_task = tokio::spawn(async move {
        crate::raft_kv::pending_txn_recovery_loop(
            recovery_raft,
            recovery_kv,
            node_id,
            std::time::Duration::from_secs(10),
            recovery_shutdown,
        )
        .await;
    });

    // Migration journal discovery: the meta leader finds and resumes
    // interrupted V2/V3 operations after restart or election. Inert until the
    // durable migration cluster level is finalized (the router gate).
    let migration_raft = raft.clone();
    let migration_manager = multi_raft.clone();
    let migration_router = raft_router.clone();
    let migration_shutdown = shutdown.clone();
    let migration_task = tokio::spawn(async move {
        crate::migration_coordinator::journal_discovery_loop(
            migration_raft,
            migration_manager,
            migration_router,
            node_id,
            std::time::Duration::from_secs(10),
            migration_shutdown,
        )
        .await;
    });

    let authz = Arc::new(nodus_authz::DefaultAuthzEngine::new(catalog.clone()));

    let executor = Arc::new(nodus_executor::MemExecutor::new(
        catalog.clone(),
        raft_catalog_writer.clone(),
        authz.clone(),
        audit_sink.clone(),
        raft_kv.clone(),
        txn,
    ));

    tracing::debug!("Executor created");

    let _executor_clone = executor.clone();
    let state_clone = state.clone();
    let reconcile_meta = meta.clone();
    let reconcile_manager = multi_raft.clone();
    tokio::spawn(async move {
        if join_peers.is_empty() {
            tracing::debug!("Background initializing raft cluster as singleton");
            let mut nodes = std::collections::BTreeMap::new();
            nodes.insert(node_id, openraft::BasicNode::new(&advertise_addr));
            let _ = raft_clone.initialize(nodes).await;
            tracing::debug!("Background raft initialization complete");

            // Wait for this node to establish leadership before bootstrapping.
            // Poll at a fine granularity (capped at ~5s) so readiness is detected
            // as soon as the election settles — single-node elections complete in
            // milliseconds — rather than waiting out a coarse fixed interval.
            let leader_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                if raft_clone.metrics().borrow().current_leader == Some(node_id) {
                    break;
                }
                if tokio::time::Instant::now() >= leader_deadline {
                    tracing::debug!("Timed out waiting for leader to establish");
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
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

            // Decided-but-incomplete cross-shard commits are re-driven by the
            // meta-leader recovery loop (`pending_txn_recovery_loop`), which
            // fires on leadership gain — including this bootstrap election —
            // and periodically thereafter.

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

    let admin_password = config.admin.password.clone().unwrap_or_else(|| {
        let generated = uuid::Uuid::new_v4().to_string();
        tracing::warn!(
            "No admin.password configured; generated random password for 'nodus' superuser: {}",
            generated
        );
        generated
    });
    // The bootstrap superuser's verifier, as PostgreSQL stores it in
    // `pg_authid.rolpassword` (its `pg_roles` row shows it as `********`).
    let admin_verifier = nodus_security::ScramKeys::derive(
        &admin_password,
        uuid::Uuid::new_v4().as_bytes().to_vec(),
        nodus_security::PBKDF2_ITERATIONS,
    )
    .to_verifier_string();
    let admin = match catalog.create_role(CreateRoleRequest {
        id: nodus_catalog::PrincipalId::new(),
        name: "nodus".into(),
        principal_type: PrincipalType::User,
        database_id: None,
        attributes: nodus_catalog::RoleAttributes {
            superuser: true,
            can_login: true,
            create_db: true,
            create_role: true,
            replication: true,
            bypass_rls: true,
            password: Some(admin_verifier),
            ..Default::default()
        },
    }) {
        Ok(desc) => desc,
        Err(e) if e.to_string().contains("already exists") => {
            catalog.get_principal_by_name("nodus")?
        }
        Err(e) => anyhow::bail!("seed admin: {e}"),
    };
    tracing::debug!("Admin seeded");

    // PostgreSQL's default ACL on a database: `PUBLIC` may connect and
    // create temporary tables. Materialized here so `has_database_privilege`
    // and `REVOKE ... FROM PUBLIC` behave as they do there.
    if let Ok(database) = catalog.get_database("default") {
        let public_id = match catalog.get_principal_by_name(nodus_catalog::PUBLIC_ROLE) {
            Ok(public) => Some(public.id),
            Err(_) => catalog
                .create_role(CreateRoleRequest {
                    id: nodus_catalog::PrincipalId::new(),
                    name: nodus_catalog::PUBLIC_ROLE.to_string(),
                    principal_type: nodus_catalog::PrincipalType::Public,
                    database_id: None,
                    attributes: Default::default(),
                })
                .ok()
                .map(|public| public.id),
        };
        if let Some(public_id) = public_id {
            let grants = catalog
                .get_grants_for_resource(nodus_catalog::ResourceRef::Database(database.id))
                .unwrap_or_default();
            for privilege in ["CONNECT", "TEMP"] {
                if !grants.iter().any(|grant| {
                    grant.principal_id == public_id
                        && grant.privilege.eq_ignore_ascii_case(privilege)
                }) {
                    let _ = catalog.grant_privilege(GrantPrivilegeRequest {
                        id: nodus_catalog::GrantId::new(),
                        principal_id: public_id,
                        resource: nodus_catalog::ResourceRef::Database(database.id),
                        privilege: privilege.to_string(),
                        grantable: false,
                        grantor: None,
                    });
                }
            }
        }
    }

    // Bootstrap superuser: ALL on System bypasses per-resource grant checks.
    let _ = catalog.grant_privilege(GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: admin.id,
        resource: ResourceRef::System,
        privilege: "ALL".into(),
        grantable: false,
        grantor: None,
    });
    // A read-only authz engine over the same catalog for the admin explain API.
    let authz = Arc::new(nodus_authz::DefaultAuthzEngine::new(catalog.clone()));
    let authenticator = Arc::new(PasswordAuthenticator::new(catalog.clone()));

    authenticator.set_bootstrap_password(&admin_password);

    // The pgwire and HTTP listeners share one TLS config and its reloadable
    // certificate resolver (rotated together on SIGHUP).
    let (server_config, http_tls_resolver) = match load_tls_config(&config.tls)? {
        Some((cfg, resolver)) => (Some(cfg), Some(resolver)),
        None => (None, None),
    };
    let tls_acceptor = server_config
        .as_ref()
        .map(|c| Arc::new(TlsAcceptor::from(c.clone())));
    tracing::debug!("TLS acceptor loaded");

    let repo = backup_repository(&config.backup.repository_uri)?;
    let backup = Arc::new(BackupOrchestrator::new(repo).with_recovery_source(local_kv.clone()));

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
                    // Fail closed: an unreadable backup repository must not
                    // collapse to `None` ("no protection"), or GC could reclaim
                    // versions a retained restore point still needs. Skip the
                    // tick instead; the next tick retries. One retention view
                    // per tick keeps the pass to a single repository scan.
                    let protected = match gc_backup.retention_view().await {
                        Ok(view) => view.protected_gc_watermark(),
                        Err(error) => {
                            tracing::warn!(
                                "backup repository unreadable; skipping GC tick: {error}"
                            );
                            continue;
                        }
                    };
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

    // Background abandoned-intent janitor: aborts pending intents whose owning
    // transaction is definitively dead (no live session here, no 2PC decision
    // record, older than txn.abandoned_intent_ttl_secs) on groups this node
    // leads, so they cannot block writers, pin MVCC GC, or stall snapshot
    // builds forever. Safety rules: `raft_kv::janitor`.
    let janitor_kv = raft_kv.clone();
    let janitor_metrics = state.metrics.clone();
    let janitor_ttl = std::time::Duration::from_secs(config.txn.abandoned_intent_ttl_secs);
    let mut janitor_shutdown = shutdown.clone();
    let janitor_task = tokio::spawn(async move {
        // Sweep often enough to converge promptly, never so often that sweeps
        // dominate: half the TTL, clamped to [1s, 30s].
        let interval = (janitor_ttl / 2).clamp(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(30),
        );
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let now = nodus_storage_api::wallclock_micros();
                    match janitor_kv.sweep_abandoned_intents(janitor_ttl, now).await {
                        Ok(report) => {
                            janitor_metrics.pending_intents.set(report.pending as i64);
                            if report.aborted > 0 {
                                janitor_metrics.janitor_aborts_total.inc_by(report.aborted as u64);
                                tracing::info!(
                                    aborted = report.aborted,
                                    pending = report.pending,
                                    "intent janitor sweep aborted abandoned intents"
                                );
                            }
                            if report.failed > 0 {
                                tracing::warn!(
                                    failed = report.failed,
                                    "intent janitor sweep had abort failures"
                                );
                            }
                        }
                        Err(e) => tracing::warn!("intent janitor sweep failed: {e}"),
                    }
                }
                _ = janitor_shutdown.changed() => {
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
            config.tls.require_tls,
            pgwire_max_connections,
            pgwire_shutdown,
        )
        .await
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Background WAL archiver. A graceful shutdown runs one final flush/pass so
    // the active segment is archived once writers have stopped; otherwise the
    // last segment could exist only on the local node.
    let backup_clone = backup.clone();
    let data_dir_clone = config.storage.data_dir.clone();
    let local_kv_clone = local_kv.clone();
    let wal_key = encryption_key;
    let mut wal_shutdown = shutdown.clone();
    let wal_archiver_task = tokio::spawn(async move {
        if let Some(dir_path_str) = data_dir_clone {
            let dir_path = std::path::PathBuf::from(dir_path_str);
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        archive_closed_wal_segments(
                            &dir_path,
                            &backup_clone,
                            &local_kv_clone,
                            wal_key,
                        )
                        .await;
                    }
                    _ = wal_shutdown.changed() => {
                        archive_closed_wal_segments(
                            &dir_path,
                            &backup_clone,
                            &local_kv_clone,
                            wal_key,
                        )
                        .await;
                        break;
                    }
                }
            }
        }
    });

    let shards = Arc::new(nodus_sharding::ShardOrchestrator::new(meta.clone()));

    let membership_lock = Arc::new(tokio::sync::Mutex::new(()));
    let raft_upgrade_coordinator = Arc::new(crate::raft_upgrade::RaftUpgradeCoordinator {
        kv: local_kv.clone(),
        manager: multi_raft.clone(),
        transport: raft_transport,
        membership_lock: membership_lock.clone(),
    });

    let admin_state = AdminState {
        registry: registry.clone(),
        audit: audit_query,
        audit_sink,
        authz: authz.clone(),
        catalog: catalog.clone(),
        catalog_writer: raft_catalog_writer.clone(),
        backup,
        upgrade: raft_upgrade_coordinator,
        shards,
        manager: multi_raft.clone(),
        router: raft_router.clone(),
        slow_log: slow_log.clone(),
        kv: executor.kv(),
        executor: executor.clone(),
        wal_key: encryption_key,
        draining: state.draining.clone(),
        authenticator: authenticator.clone(),
        admin_token: config.admin.token.clone(),
        allow_insecure: config.admin.allow_insecure,
        raft_state: raft_state.clone(),
        membership_lock,
        restore_lock: Arc::new(tokio::sync::Mutex::new(())),
        restoring: executor.restoring_flag(),
        restore_gate: executor.restore_gate(),
        restore_in_flight: Arc::new(parking_lot::Mutex::new(None)),
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
        // On the shared listener the Raft endpoints are reachable by anything
        // that can reach the admin API, so they require the same credential.
        app = app.merge(
            nodus_raftstore::server::raft_routes(raft_route_auth(&config))
                .with_state(raft_state.clone()),
        );
    }
    let app = app.layer(cors);

    // Dedicated Raft peer listener (separate from admin/web), with mandatory
    // client-certificate mTLS when `cluster.tls` is enabled.
    let mut raft_tls_resolver: Option<Arc<ReloadableCertResolver>> = None;
    let raft_serve_task = if let Some(addr) = raft_listen_addr.clone() {
        let raft_tls = load_raft_tls_config(&config.cluster.tls)?;
        raft_tls_resolver = raft_tls.as_ref().map(|(_, resolver)| resolver.clone());
        let raft_server_config = raft_tls.map(|(cfg, _)| cfg);
        // With mTLS the TLS handshake (cluster-CA client certificate) is the
        // peer credential; without it the listener falls back to the same
        // bearer-token requirement as the public app.
        let raft_auth = if raft_server_config.is_some() {
            nodus_raftstore::server::RaftRouteAuth::TransportAuthenticated
        } else {
            raft_route_auth(&config)
        };
        let raft_listener = match raft_listener {
            Some(listener) => listener,
            None => TcpListener::bind(&addr).await?,
        };
        let raft_app = Router::new()
            .merge(nodus_raftstore::server::raft_routes(raft_auth).with_state(raft_state));
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

    // Hot certificate rotation: on SIGHUP, re-read every TLS listener's
    // certificate from disk (operators replace the files in place, then signal).
    // A reload that fails to parse keeps the previous certificate serving.
    #[cfg(unix)]
    {
        let resolvers: Vec<Arc<ReloadableCertResolver>> =
            [http_tls_resolver.clone(), raft_tls_resolver.clone()]
                .into_iter()
                .flatten()
                .collect();
        if !resolvers.is_empty() {
            let mut sighup_shutdown = shutdown.clone();
            tokio::spawn(async move {
                let mut hup =
                    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
                        Ok(sig) => sig,
                        Err(e) => {
                            tracing::error!("failed to install SIGHUP handler for TLS reload: {e}");
                            return;
                        }
                    };
                loop {
                    tokio::select! {
                        _ = hup.recv() => {
                            for resolver in &resolvers {
                                match resolver.reload() {
                                    Ok(()) => tracing::info!("reloaded TLS certificate on SIGHUP"),
                                    Err(e) => tracing::error!(
                                        "TLS certificate reload failed, keeping previous: {e}"
                                    ),
                                }
                            }
                        }
                        _ = sighup_shutdown.changed() => break,
                    }
                }
            });
        }
    }
    // `http_tls_resolver` is consumed by the SIGHUP task on unix; reference it on
    // other platforms so it isn't flagged unused.
    #[cfg(not(unix))]
    let _ = &http_tls_resolver;

    let metrics_state = state.clone();
    let metrics_manager = multi_raft.clone();
    let metrics_node_id = config.cluster.node_id;
    let mut raft_metrics = raft.metrics();
    let mut rm_shutdown = shutdown.clone();
    tokio::spawn(async move {
        use std::sync::atomic::Ordering::Relaxed;
        let mut shard_tick = tokio::time::interval(std::time::Duration::from_secs(2));
        // Per-group snapshots of the last reported deferral counters (to turn
        // cumulative counts into deltas) and last warning time (to throttle
        // the log-growth warning to one line per group per minute).
        let mut deferral_counts: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();
        let mut deferral_warned: std::collections::HashMap<String, tokio::time::Instant> =
            std::collections::HashMap::new();
        /// Warn when a group's snapshot build has deferred continuously this
        /// long: while deferred the Raft log cannot be purged and grows with
        /// every write.
        const DEFERRAL_WARN_AFTER: std::time::Duration = std::time::Duration::from_secs(600);
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

                    // Log-growth visibility: retained Raft log entries per group.
                    for (group, retained) in metrics_manager.log_retention().await {
                        metrics_state
                            .metrics
                            .raft_log_entries
                            .get_or_create(&vec![("group".to_string(), group)])
                            .set(retained as i64);
                    }
                    // Snapshot-build deferrals: cumulative count plus the current
                    // streak's duration, and a (throttled) warning past the
                    // threshold.
                    let deferrals = metrics_manager.snapshot_deferrals();
                    for (group, deferral) in &deferrals {
                        let labels = vec![("group".to_string(), group.clone())];
                        let previous = deferral_counts.get(group).copied().unwrap_or(0);
                        if deferral.count > previous {
                            metrics_state
                                .metrics
                                .snapshot_deferrals_total
                                .get_or_create(&labels)
                                .inc_by(deferral.count - previous);
                            deferral_counts.insert(group.clone(), deferral.count);
                        }
                        let deferred_for = deferral.since.elapsed();
                        metrics_state
                            .metrics
                            .snapshot_deferred_seconds
                            .get_or_create(&labels)
                            .set(deferred_for.as_secs() as i64);
                        if deferred_for >= DEFERRAL_WARN_AFTER {
                            let quiet = deferral_warned
                                .get(group)
                                .is_none_or(|at| at.elapsed() >= std::time::Duration::from_secs(60));
                            if quiet {
                                tracing::warn!(
                                    group,
                                    deferred_secs = deferred_for.as_secs(),
                                    reason = deferral.reason,
                                    deferrals = deferral.count,
                                    "snapshot build keeps deferring; the group's Raft log grows until it succeeds"
                                );
                                deferral_warned.insert(group.clone(), tokio::time::Instant::now());
                            }
                        }
                    }
                    // Groups whose builds now succeed (or which are gone) drop
                    // out of the deferral map: reset their streak gauge and the
                    // delta bookkeeping so a later streak counts from zero.
                    let active: std::collections::HashSet<&str> = deferrals
                        .iter()
                        .map(|(group, _)| group.as_str())
                        .collect();
                    deferral_counts.retain(|group, _| active.contains(group.as_str()));
                    deferral_warned.retain(|group, _| active.contains(group.as_str()));
                    for group in metrics_manager.hosted_group_ids() {
                        if !active.contains(group.as_str()) {
                            metrics_state
                                .metrics
                                .snapshot_deferred_seconds
                                .get_or_create(&vec![("group".to_string(), group)])
                                .set(0);
                        }
                    }
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
        #[cfg(any(test, feature = "test-utils"))]
        catalog,
        pgwire_addr,
        http_addr,
        pgwire_task,
        http_task,
        background_tasks: {
            let mut tasks = vec![
                gc_task,
                janitor_task,
                recovery_task,
                migration_task,
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
    pub(crate) fn write_test_certs(dir: &std::path::Path) -> (String, String, String) {
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        // rcgen 0.14: a cert is signed by an `Issuer` (params + signing key),
        // not by passing the CA cert and key separately.
        let ca_issuer = rcgen::Issuer::new(ca_params, ca_key);

        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let mut leaf_params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        leaf_params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_issuer).unwrap();

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

        // Server: serve the Raft routes with mandatory client-cert mTLS. The
        // certificate is the peer credential on this listener, so the routes
        // run in transport-authenticated mode (no bearer token required).
        let (server_cfg, _resolver) = load_raft_tls_config(&cluster.tls).unwrap().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().merge(
            nodus_raftstore::server::raft_routes(
                nodus_raftstore::server::RaftRouteAuth::TransportAuthenticated,
            )
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
            .tls_certs_only([ca.clone()])
            .identity(reqwest::Identity::from_pem(&identity_pem).unwrap())
            .build()
            .unwrap();
        let resp = mtls_client.post(&url).json(&body).send().await;
        let status = resp.expect("mTLS peer should connect").status();
        assert_eq!(status, reqwest::StatusCode::NOT_FOUND);

        // A client with no certificate is rejected at the TLS layer.
        let no_cert_client = reqwest::Client::builder()
            .use_rustls_tls()
            .tls_certs_only([ca])
            .build()
            .unwrap();
        assert!(
            no_cert_client.post(&url).json(&body).send().await.is_err(),
            "a peer without a client certificate must be rejected"
        );
    }

    /// Raft routes on a non-mTLS listener require the admin bearer token: no
    /// token and wrong tokens get 401, the right token reaches the handler
    /// (unknown shard → 404), and anonymous access works only under the
    /// explicit `allow_insecure` opt-in.
    #[tokio::test]
    async fn raft_routes_require_admin_bearer_token() {
        use nodus_raftstore::server::{RaftRouteAuth, RaftState, raft_routes};
        let body = serde_json::json!({
            "vote": {"leader_id": {"term": 1, "node_id": 1}, "committed": false},
            "last_log_id": null
        });
        let serve = |auth: RaftRouteAuth| async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let app = Router::new().merge(raft_routes(auth).with_state(RaftState::new()));
            tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            addr
        };

        let addr = serve(RaftRouteAuth::AdminToken {
            token: Some("t0ken".into()),
            allow_insecure: false,
        })
        .await;
        let url = format!("http://{addr}/raft/shard-meta/vote");
        let http = reqwest::Client::new();
        assert_eq!(
            http.post(&url).json(&body).send().await.unwrap().status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "anonymous raft RPC must be rejected"
        );
        assert_eq!(
            http.post(&url)
                .bearer_auth("wrong")
                .json(&body)
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            http.post(&url)
                .bearer_auth("t0ken")
                .json(&body)
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::NOT_FOUND,
            "authenticated request reaches the handler (unknown shard)"
        );

        // No token configured and no insecure opt-in: still denied.
        let addr = serve(RaftRouteAuth::AdminToken {
            token: None,
            allow_insecure: false,
        })
        .await;
        let url = format!("http://{addr}/raft/shard-meta/vote");
        assert_eq!(
            http.post(&url).json(&body).send().await.unwrap().status(),
            reqwest::StatusCode::UNAUTHORIZED
        );

        // Explicit opt-in mirrors the admin API's anonymous-superuser escape.
        let addr = serve(RaftRouteAuth::AdminToken {
            token: None,
            allow_insecure: true,
        })
        .await;
        let url = format!("http://{addr}/raft/shard-meta/vote");
        assert_eq!(
            http.post(&url).json(&body).send().await.unwrap().status(),
            reqwest::StatusCode::NOT_FOUND
        );
    }

    /// Stores a valid finalized upgrade-authority record at the migration
    /// cluster level — the durable state a level-3 node serves from.
    pub(crate) fn store_finalized_level3(kv: &dyn nodus_storage_api::KvEngine) {
        use nodus_raftstore::upgrade::{CapabilityV1, MIGRATION_LEVEL, Phase, RecordV1};
        use nodus_storage_api::TxnId;
        use openraft::{BasicNode, CommittedLeaderId, LogId, Membership, StoredMembership};
        let session = uuid::Uuid::new_v4();
        let mut report = CapabilityV1::local(1, session);
        report.ready_for_finalize = true;
        let record = RecordV1 {
            version: 1,
            revision: 3,
            phase: Phase::Finalized,
            cluster_version: MIGRATION_LEVEL,
            session,
            membership: StoredMembership::new(
                Some(LogId::new(CommittedLeaderId::new(1, 1), 1)),
                Membership::new(
                    vec![[1].into()],
                    std::collections::BTreeMap::from([(1, BasicNode::new("one"))]),
                ),
            ),
            reports: std::collections::BTreeMap::from([(1, report)]),
        };
        let txn = TxnId::new();
        kv.write_intent(
            txn,
            bytes::Bytes::from_static(nodus_raftstore::upgrade::KEY),
            bytes::Bytes::from(serde_json::to_vec(&record).unwrap()),
        )
        .unwrap();
        kv.commit(txn, 1).unwrap();
    }

    /// Migration-protocol forwarding end to end over real mTLS: the recipient's
    /// gate opens only with a finalized level-3 durable authority, the probe
    /// verifies the peer's capability and challenge, and the plain-HTTP
    /// transport refuses to carry the command at all.
    #[tokio::test]
    async fn migration_forwarding_uses_mtls_and_recipient_level_gate() {
        use nodus_raftstore::ShardCommand;
        use nodus_raftstore::server::{RaftRouteAuth, RaftState, raft_routes};

        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path, ca_path) = write_test_certs(dir.path());
        let cluster = nodus_config::ClusterConfig {
            tls: nodus_config::ClusterTlsConfig {
                enabled: true,
                cert_path: Some(cert_path),
                key_path: Some(key_path),
                ca_path: Some(ca_path),
            },
            raft_listen_addr: Some("127.0.0.1:0".into()),
            ..Default::default()
        };
        let transport = build_raft_transport(&cluster).unwrap();
        assert_eq!(transport.scheme(), "https");

        // Serves the raft routes over mandatory-mTLS with the given authority state.
        let serve_mtls = |level3: bool| {
            let cluster = cluster.clone();
            async move {
                let kv = Arc::new(nodus_storage_mem::MemKvEngine::new());
                if level3 {
                    store_finalized_level3(kv.as_ref());
                }
                let state = RaftState::new();
                state.set_node_id(1);
                state.set_capability_storage(kv);
                let (server_cfg, _resolver) = load_raft_tls_config(&cluster.tls).unwrap().unwrap();
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let app = Router::new()
                    .merge(raft_routes(RaftRouteAuth::TransportAuthenticated).with_state(state));
                let tls_config = axum_server::tls_rustls::RustlsConfig::from_config(server_cfg);
                tokio::spawn(async move {
                    let _ = axum_server::from_tcp_rustls(listener.into_std().unwrap(), tls_config)
                        .unwrap()
                        .serve(app.into_make_service())
                        .await;
                });
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                addr
            }
        };

        let migration_cmd = ShardCommand::EpochPrepareV2 {
            epoch: 1,
            txn_id: uuid::Uuid::new_v4(),
            expect_intents: false,
        };

        // The plain-HTTP transport refuses to carry migration-protocol commands.
        let plain = nodus_raftstore::network::RaftTransport::plain();
        let error = plain
            .forward_write("127.0.0.1:1", "shard-x", &migration_cmd, "r1")
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("requires peer mTLS"),
            "plain transport must refuse: {error}"
        );

        // Level-3 recipient: capability probe validates, and the forwarded
        // command passes the ingress gate (unknown shard then yields 404).
        let addr = serve_mtls(true).await;
        let report = transport
            .probe(1, &addr.to_string(), uuid::Uuid::new_v4(), false)
            .await
            .unwrap();
        assert_eq!(
            report.migration_version,
            Some(nodus_raftstore::upgrade::MIGRATION_VERSION)
        );
        let error = transport
            .forward_write(&addr.to_string(), "shard-x", &migration_cmd, "r2")
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("404"),
            "finalized level-3 recipient passes the gate, then 404: {error}"
        );

        // Level-1 recipient: the ingress gate stays closed (501).
        let addr = serve_mtls(false).await;
        let error = transport
            .forward_write(&addr.to_string(), "shard-x", &migration_cmd, "r3")
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("501"),
            "unfinalized recipient rejects with 501: {error}"
        );
    }

    /// The router's production gate follows the durable upgrade authority:
    /// closed at level 1, opened by the finalized level-3 record, with no
    /// config or environment bypass.
    #[tokio::test]
    async fn router_migration_gate_follows_the_durable_authority() {
        use nodus_raftstore::ShardCommand;
        use nodus_storage_api::KvEngine;

        let base: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let manager = Arc::new(crate::multi_raft::MultiRaftManager::new(
            1,
            "127.0.0.1:0".into(),
            Arc::new(openraft::Config::default().validate().unwrap()),
            nodus_raftstore::server::RaftState::new(),
            base.clone(),
            None,
            Arc::new(nodus_txn::MemTxnManager::new()),
            None,
            Default::default(),
        ));
        let gate_kv = base.clone();
        let router =
            crate::raft_router::RaftRouter::spawn(manager).with_migration_gate(move || {
                nodus_raftstore::upgrade::read(gate_kv.as_ref()).is_ok_and(|authority| {
                    authority.cluster_version >= nodus_raftstore::upgrade::MIGRATION_LEVEL
                })
            });
        let command = || ShardCommand::EpochPrepareV2 {
            epoch: 1,
            txn_id: uuid::Uuid::new_v4(),
            expect_intents: false,
        };

        // Level 1 (no authority record): the gate rejects before dispatch.
        let router2 = router.clone();
        let error = tokio::task::spawn_blocking(move || router2.submit("shard-x", command()))
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("migration protocol activation requires"),
            "closed gate rejects: {error}"
        );

        // Finalized level 3: the gate opens; the command reaches the
        // dispatcher and fails only because the group does not exist.
        store_finalized_level3(base.as_ref());
        let error = tokio::task::spawn_blocking(move || router.submit("shard-x", command()))
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            error.to_string().contains("not found"),
            "open gate lets the command through: {error}"
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
            require_tls: false,
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
    fn backup_repository_uri_dispatch() {
        // file:// and bare paths stay on the filesystem backend.
        assert_eq!(
            parse_backup_repository_uri("file:///var/lib/nodus/backups").unwrap(),
            BackupRepositoryKind::Fs(PathBuf::from("/var/lib/nodus/backups"))
        );
        assert_eq!(
            parse_backup_repository_uri("/var/lib/nodus/backups").unwrap(),
            BackupRepositoryKind::Fs(PathBuf::from("/var/lib/nodus/backups"))
        );
        assert_eq!(
            parse_backup_repository_uri("relative/dir").unwrap(),
            BackupRepositoryKind::Fs(PathBuf::from("relative/dir"))
        );
        // s3://bucket[/prefix] selects the object-store backend.
        assert_eq!(
            parse_backup_repository_uri("s3://nodus-backups").unwrap(),
            BackupRepositoryKind::S3 {
                bucket: "nodus-backups".into(),
                prefix: String::new(),
            }
        );
        assert_eq!(
            parse_backup_repository_uri("s3://nodus-backups/prod/cluster-a").unwrap(),
            BackupRepositoryKind::S3 {
                bucket: "nodus-backups".into(),
                prefix: "prod/cluster-a".into(),
            }
        );
        // s3:// without a bucket is a startup error.
        let error = parse_backup_repository_uri("s3://")
            .unwrap_err()
            .to_string();
        assert!(error.contains("bucket"), "{error}");
        // Unknown schemes fail startup with an actionable error instead of
        // silently becoming a local directory named e.g. "gs:/bucket".
        let error = parse_backup_repository_uri("gs://bucket")
            .unwrap_err()
            .to_string();
        assert!(error.contains("gs"), "{error}");
        assert!(error.contains("file://"), "{error}");
        assert!(error.contains("s3://"), "{error}");
        assert!(parse_backup_repository_uri("http://example.com/backups").is_err());
    }

    #[test]
    fn backup_repository_constructs_selected_backend() {
        // Filesystem backend constructs for file://, bare paths, and the empty
        // dev default.
        backup_repository("file:///var/lib/nodus/backups").unwrap();
        backup_repository("relative/dir").unwrap();
        backup_repository("").unwrap();
        // Unknown schemes error before any construction.
        assert!(backup_repository("gs://bucket").is_err());
    }

    #[test]
    fn backup_repository_constructs_s3_from_env() {
        // SAFETY: no other test in this crate reads or writes these variables;
        // they are set before construction and removed after.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "nodus-test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "nodus-test-secret");
            std::env::set_var("AWS_REGION", "us-east-1");
        }
        let result = backup_repository("s3://nodus-test-bucket/prod");
        unsafe {
            std::env::remove_var("AWS_ACCESS_KEY_ID");
            std::env::remove_var("AWS_SECRET_ACCESS_KEY");
            std::env::remove_var("AWS_REGION");
        }
        let repo = result.unwrap();
        assert!(repo.capabilities().range_reads);
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
            started_at: None,
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
