mod s3;
pub use s3::{S3BackupRepository, S3Config};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use nodus_storage_api::KvEngine;
use nodus_storage_wal::{FileWalEngine, WalEngine, WalRecord, WalRecordV1};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct PutOptions {
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepositoryCapabilities {
    pub immutable_objects: bool,
    pub conditional_put: bool,
    pub range_reads: bool,
    pub multipart_upload: bool,
    pub server_side_encryption: bool,
}

impl Default for RepositoryCapabilities {
    fn default() -> Self {
        Self {
            immutable_objects: false,
            conditional_put: false,
            range_reads: true,
            multipart_upload: false,
            server_side_encryption: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64, // inclusive
}

#[async_trait]
pub trait BackupRepository: Send + Sync {
    fn capabilities(&self) -> RepositoryCapabilities {
        RepositoryCapabilities::default()
    }

    async fn put_object(
        &self,
        key: &str,
        body: Bytes,
        options: PutOptions,
    ) -> Result<ObjectMetadata>;
    async fn get_object(&self, key: &str, range: Option<ByteRange>) -> Result<Bytes>;
    async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectMetadata>>;
    async fn delete_object(&self, key: &str) -> Result<()>;
    async fn object_exists(&self, key: &str) -> Result<bool>;
}

// In-Memory MVP Repository for tests
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::RwLock;

pub struct MemBackupRepository {
    objects: RwLock<HashMap<String, Bytes>>,
}

impl Default for MemBackupRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl MemBackupRepository {
    pub fn new() -> Self {
        Self {
            objects: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl BackupRepository for MemBackupRepository {
    fn capabilities(&self) -> RepositoryCapabilities {
        RepositoryCapabilities::default()
    }

    async fn put_object(
        &self,
        key: &str,
        body: Bytes,
        _options: PutOptions,
    ) -> Result<ObjectMetadata> {
        let size = body.len() as u64;
        self.objects.write().unwrap().insert(key.to_string(), body);
        Ok(ObjectMetadata {
            key: key.to_string(),
            size,
            last_modified: Utc::now(),
        })
    }

    async fn get_object(&self, key: &str, _range: Option<ByteRange>) -> Result<Bytes> {
        self.objects
            .read()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Object {} not found", key))
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectMetadata>> {
        let mut res = Vec::new();
        for (k, v) in self.objects.read().unwrap().iter() {
            if k.starts_with(prefix) {
                res.push(ObjectMetadata {
                    key: k.clone(),
                    size: v.len() as u64,
                    last_modified: Utc::now(),
                });
            }
        }
        Ok(res)
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        self.objects.write().unwrap().remove(key);
        Ok(())
    }

    async fn object_exists(&self, key: &str) -> Result<bool> {
        Ok(self.objects.read().unwrap().contains_key(key))
    }
}

// Filesystem-backed repository. Objects are stored as files under `root`, using
// the object key as a relative path. Suitable for single-node and
// volume-mounted (NFS/EBS) production backups.
use std::path::{Path, PathBuf};

pub struct FsBackupRepository {
    root: PathBuf,
}

impl FsBackupRepository {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path_for(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    fn collect(dir: &Path, root: &Path, out: &mut Vec<ObjectMetadata>) -> Result<()> {
        if !dir.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                Self::collect(&path, root, out)?;
            } else {
                let meta = entry.metadata()?;
                let key = path
                    .strip_prefix(root)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .replace('\\', "/");
                out.push(ObjectMetadata {
                    key,
                    size: meta.len(),
                    last_modified: Utc::now(),
                });
            }
        }
        Ok(())
    }
}

#[async_trait]
impl BackupRepository for FsBackupRepository {
    fn capabilities(&self) -> RepositoryCapabilities {
        RepositoryCapabilities::default()
    }

    async fn put_object(
        &self,
        key: &str,
        body: Bytes,
        _options: PutOptions,
    ) -> Result<ObjectMetadata> {
        let path = self.path_for(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let size = body.len() as u64;
        tokio::fs::write(&path, &body).await?;
        Ok(ObjectMetadata {
            key: key.to_string(),
            size,
            last_modified: Utc::now(),
        })
    }

    async fn get_object(&self, key: &str, range: Option<ByteRange>) -> Result<Bytes> {
        let path = self.path_for(key);
        let data = tokio::fs::read(&path)
            .await
            .map_err(|e| anyhow::anyhow!("Object {} not found: {}", key, e))?;
        let bytes = Bytes::from(data);
        match range {
            Some(r) => {
                let end = (r.end as usize + 1).min(bytes.len());
                let start = (r.start as usize).min(end);
                Ok(bytes.slice(start..end))
            }
            None => Ok(bytes),
        }
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectMetadata>> {
        let root = self.root.clone();
        let mut all = Vec::new();
        Self::collect(&root, &root, &mut all)?;
        all.retain(|o| o.key.starts_with(prefix));
        Ok(all)
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        let path = self.path_for(key);
        if path.exists() {
            tokio::fs::remove_file(&path).await?;
        }
        Ok(())
    }

    async fn object_exists(&self, key: &str) -> Result<bool> {
        Ok(self.path_for(key).exists())
    }
}

// Models
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BackupType {
    Full,
    Incremental,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BackupStatus {
    Pending,
    ChoosingTimestamp,
    ExportingCatalog,
    ExportingTxnRecords,
    ExportingShards,
    Uploading,
    WritingManifest,
    Verifying,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalPosition {
    pub timeline_id: String,
    pub segment_id: String,
    pub offset: u64,
    pub commit_ts: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompressionMetadata {
    pub algorithm: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncryptionMetadata {
    pub key_id: Option<String>,
    pub method: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestoreValidationStatus {
    pub validated_at: DateTime<Utc>,
    pub level: String,
    pub passed: bool,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProtectedTimestamp {
    pub id: String,
    pub lower_bound_ts: u64,
    pub upper_bound_ts: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalArchiveUploadState {
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalCommittedTxn {
    pub txn_id: String,
    pub commit_ts: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalArchiveIndex {
    pub index_version: u64,
    pub timeline_id: String,
    pub segment_id: String,
    pub wal_object_key: String,
    pub index_object_key: String,
    pub size: u64,
    pub checksum: String,
    pub first_commit_ts: Option<u64>,
    pub last_commit_ts: Option<u64>,
    pub record_txn_ids: Vec<String>,
    pub committed_txns: Vec<WalCommittedTxn>,
    /// The WAL-segment id this segment follows, recorded so PITR can verify an
    /// unbroken lineage across the (sparse) segment-id space. `None` for the
    /// first segment, or a segment archived before lineage was recorded.
    #[serde(default)]
    pub predecessor: Option<u64>,
    pub archived_at: DateTime<Utc>,
    pub upload_state: WalArchiveUploadState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PitrRestoreWalSegment {
    pub segment_id: String,
    pub wal_object_key: String,
    pub index_object_key: String,
    pub first_commit_ts: Option<u64>,
    pub last_commit_ts: Option<u64>,
    pub checksum: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PitrRestorePlan {
    pub plan_version: u64,
    pub cluster_id: String,
    pub timeline_id: String,
    pub base_backup_id: String,
    pub base_backup_chain: Vec<String>,
    pub base_snapshot_ts: u64,
    pub target_ts: u64,
    pub replay_start_ts: u64,
    pub replay_end_ts: u64,
    pub wal_segments: Vec<PitrRestoreWalSegment>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PitrWalSegmentBytes {
    pub segment_id: String,
    pub bytes: Bytes,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PitrReplayReport {
    pub base_objects_restored: usize,
    pub base_kv_versions_restored: usize,
    pub wal_segments_replayed: usize,
    pub records_seen: usize,
    pub writes_applied: usize,
    pub deletes_applied: usize,
    pub commits_applied: usize,
    pub commits_skipped: usize,
    pub aborts_applied: usize,
    pub pending_txns_aborted: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    pub manifest_version: u64,
    pub repository_layout_version: u64,
    pub object_format_version: u64,
    pub backup_id: String,
    pub cluster_id: String,
    pub timeline_id: String,
    pub backup_type: BackupType,
    pub parent_backup_id: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub snapshot_ts: u64,
    pub backup_ts: u64,
    pub catalog_version: u64,
    pub cluster_version: u64,
    pub wal_start: Option<WalPosition>,
    pub wal_end: Option<WalPosition>,
    pub files: Vec<String>,
    pub checksums: HashMap<String, String>,
    pub compression: Option<CompressionMetadata>,
    pub encryption: Option<EncryptionMetadata>,
    pub restore_validation: Option<RestoreValidationStatus>,
    pub protected_timestamp: Option<ProtectedTimestamp>,
    pub repository_capabilities: RepositoryCapabilities,
    pub status: BackupStatus,
}

// Orchestration
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

const CURRENT_MANIFEST_VERSION: u64 = 2;
const CURRENT_REPOSITORY_LAYOUT_VERSION: u64 = 2;
const CURRENT_OBJECT_FORMAT_VERSION: u64 = 1;
const CURRENT_WAL_ARCHIVE_INDEX_VERSION: u64 = 1;
const CURRENT_PITR_RESTORE_PLAN_VERSION: u64 = 1;
const DEFAULT_TIMELINE_ID: &str = "default";

fn checksum(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    // sha2 0.11 returns a `hybrid_array::Array` (no `LowerHex`); hex-encode bytes.
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn pending_manifest_key(backup_id: &str) -> String {
    format!("{backup_id}/manifest.pending.json")
}

fn manifest_key(backup_id: &str) -> String {
    format!("{backup_id}/manifest.json")
}

fn data_key(backup_id: &str, name: &str) -> String {
    format!("{backup_id}/data/{name}")
}

fn wal_key(filename: &str) -> String {
    format!("wal_archive/{filename}")
}

fn wal_index_key(filename: &str) -> String {
    format!("wal_archive/{filename}.index.json")
}

fn wal_segment_sequence(segment_id: &str) -> Result<u64> {
    segment_id
        .strip_suffix(".log")
        .unwrap_or(segment_id)
        .parse::<u64>()
        .map_err(|_| anyhow::anyhow!("unsupported non-numeric WAL segment id {segment_id}"))
}

fn recover_wal_records(
    segment_id: &str,
    bytes: &Bytes,
    wal_key: Option<[u8; 32]>,
) -> Result<Vec<WalRecord>> {
    let path = std::env::temp_dir().join(format!(
        "nodus-pitr-replay-{}-{}",
        segment_id.replace('/', "_"),
        Uuid::new_v4()
    ));
    std::fs::write(&path, bytes)?;
    let result = (|| {
        let wal = FileWalEngine::with_encryption(&path, wal_key)?;
        wal.recover()
    })();
    let _ = std::fs::remove_file(&path);
    result
}

fn json_bytes_field(value: &serde_json::Value, field: &str) -> Option<Bytes> {
    let bytes = value
        .get(field)?
        .as_array()?
        .iter()
        .map(|byte| byte.as_u64().and_then(|byte| u8::try_from(byte).ok()))
        .collect::<Option<Vec<_>>>()?;
    Some(Bytes::from(bytes))
}

/// A unit of data to back up: a logical name and its bytes (e.g. a shard export
/// or the serialized catalog).
pub struct BackupObject {
    pub name: String,
    pub bytes: Bytes,
}

struct ManifestTemplate {
    backup_id: String,
    cluster_id: String,
    backup_type: BackupType,
    parent_backup_id: Option<String>,
    started_at: DateTime<Utc>,
    backup_ts: u64,
    catalog_version: u64,
    cluster_version: u64,
    files: Vec<String>,
    checksums: HashMap<String, String>,
    protected_timestamp: Option<ProtectedTimestamp>,
    status: BackupStatus,
}

/// Drives backups and restores against a [`BackupRepository`], writing a
/// versioned manifest and per-file SHA-256 checksums. The manifest is only
/// marked `Completed` after every object and the manifest itself are durably
/// written and re-verified, upholding the invariant that a backup reported
/// COMPLETE is restorable.
pub struct BackupOrchestrator {
    repo: Arc<dyn BackupRepository>,
}

impl BackupOrchestrator {
    pub fn new(repo: Arc<dyn BackupRepository>) -> Self {
        Self { repo }
    }

    fn manifest_template(&self, input: ManifestTemplate) -> BackupManifest {
        BackupManifest {
            manifest_version: CURRENT_MANIFEST_VERSION,
            repository_layout_version: CURRENT_REPOSITORY_LAYOUT_VERSION,
            object_format_version: CURRENT_OBJECT_FORMAT_VERSION,
            backup_id: input.backup_id,
            cluster_id: input.cluster_id,
            timeline_id: DEFAULT_TIMELINE_ID.to_string(),
            backup_type: input.backup_type,
            parent_backup_id: input.parent_backup_id,
            started_at: input.started_at,
            completed_at: None,
            snapshot_ts: input.backup_ts,
            backup_ts: input.backup_ts,
            catalog_version: input.catalog_version,
            cluster_version: input.cluster_version,
            wal_start: None,
            wal_end: None,
            files: input.files,
            checksums: input.checksums,
            compression: None,
            encryption: None,
            restore_validation: None,
            protected_timestamp: input.protected_timestamp,
            repository_capabilities: self.repo.capabilities(),
            status: input.status,
        }
    }

    async fn put_manifest(&self, key: &str, manifest: &BackupManifest) -> Result<()> {
        let body = serde_json::to_vec(manifest)?;
        self.repo
            .put_object(
                key,
                Bytes::from(body),
                PutOptions {
                    content_type: Some("application/json".into()),
                },
            )
            .await
            .map(|_| ())
    }

    async fn put_final_manifest(&self, manifest: &BackupManifest) -> Result<()> {
        let key = manifest_key(&manifest.backup_id);
        if self.repo.object_exists(&key).await? {
            anyhow::bail!(
                "completed manifest already exists for {}",
                manifest.backup_id
            );
        }
        self.put_manifest(&key, manifest).await
    }

    async fn verify_manifest_files(&self, manifest: &BackupManifest) -> Result<()> {
        for key in &manifest.files {
            let bytes = self.repo.get_object(key, None).await?;
            let expected = manifest
                .checksums
                .get(key)
                .ok_or_else(|| anyhow::anyhow!("missing checksum for {key}"))?;
            if &checksum(&bytes) != expected {
                anyhow::bail!("checksum mismatch for {key}");
            }
        }
        Ok(())
    }

    async fn upload_objects(
        &self,
        backup_id: &str,
        objects: &[BackupObject],
    ) -> Result<(Vec<String>, HashMap<String, String>)> {
        let mut files = Vec::new();
        let mut checksums = HashMap::new();
        for obj in objects {
            let key = data_key(backup_id, &obj.name);
            self.repo
                .put_object(&key, obj.bytes.clone(), PutOptions::default())
                .await?;
            checksums.insert(key.clone(), checksum(&obj.bytes));
            files.push(key);
        }
        Ok((files, checksums))
    }

    fn protected_timestamp(
        &self,
        backup_id: &str,
        lower_bound_ts: u64,
        upper_bound_ts: u64,
    ) -> ProtectedTimestamp {
        ProtectedTimestamp {
            id: format!("backup-{backup_id}"),
            lower_bound_ts,
            upper_bound_ts,
            created_at: Utc::now(),
        }
    }

    pub async fn create_full_backup(
        &self,
        cluster_id: &str,
        backup_ts: u64,
        catalog_version: u64,
        cluster_version: u64,
        objects: Vec<BackupObject>,
    ) -> Result<BackupManifest> {
        let backup_id = Uuid::new_v4().to_string();
        let started_at = Utc::now();
        let (files, checksums) = self.upload_objects(&backup_id, &objects).await?;

        let pending = self.manifest_template(ManifestTemplate {
            backup_id: backup_id.clone(),
            cluster_id: cluster_id.to_string(),
            backup_type: BackupType::Full,
            parent_backup_id: None,
            started_at,
            backup_ts,
            catalog_version,
            cluster_version,
            files,
            checksums,
            protected_timestamp: Some(self.protected_timestamp(&backup_id, 0, backup_ts)),
            status: BackupStatus::Verifying,
        });

        self.put_manifest(&pending_manifest_key(&backup_id), &pending)
            .await?;
        self.verify_manifest_files(&pending).await?;

        let mut completed = pending;
        completed.completed_at = Some(Utc::now());
        completed.status = BackupStatus::Completed;
        self.put_final_manifest(&completed).await?;
        self.verify(&backup_id).await?;
        Ok(completed)
    }

    pub async fn load_manifest(&self, backup_id: &str) -> Result<BackupManifest> {
        let body = self.repo.get_object(&manifest_key(backup_id), None).await?;
        let manifest = serde_json::from_slice::<BackupManifest>(&body)?;
        if manifest.manifest_version != CURRENT_MANIFEST_VERSION {
            anyhow::bail!(
                "unsupported backup manifest version {} for backup {}",
                manifest.manifest_version,
                backup_id
            );
        }
        Ok(manifest)
    }

    /// Verifies that the manifest is `Completed` and every recorded file is
    /// present with a matching checksum. Returns an error otherwise.
    pub async fn verify(&self, backup_id: &str) -> Result<()> {
        let manifest = self.load_manifest(backup_id).await?;
        if manifest.status != BackupStatus::Completed {
            anyhow::bail!("backup {backup_id} is not COMPLETE: {:?}", manifest.status);
        }
        self.verify_manifest_files(&manifest).await
    }

    pub async fn create_incremental_backup(
        &self,
        cluster_id: &str,
        parent_backup_id: &str,
        backup_ts: u64,
        catalog_version: u64,
        cluster_version: u64,
        objects: Vec<BackupObject>,
    ) -> Result<BackupManifest> {
        let backup_id = Uuid::new_v4().to_string();
        let started_at = Utc::now();

        if objects.is_empty() {
            anyhow::bail!("incremental backup requires at least one changed object");
        }

        self.verify(parent_backup_id).await?;
        let parent = self.load_manifest(parent_backup_id).await?;
        if parent.cluster_id != cluster_id {
            anyhow::bail!(
                "parent backup {} belongs to cluster {}, not {}",
                parent_backup_id,
                parent.cluster_id,
                cluster_id
            );
        }
        if backup_ts <= parent.snapshot_ts {
            anyhow::bail!(
                "incremental backup timestamp {} must be greater than parent snapshot {}",
                backup_ts,
                parent.snapshot_ts
            );
        }
        let (files, checksums) = self.upload_objects(&backup_id, &objects).await?;

        let pending = self.manifest_template(ManifestTemplate {
            backup_id: backup_id.clone(),
            cluster_id: cluster_id.to_string(),
            backup_type: BackupType::Incremental,
            parent_backup_id: Some(parent_backup_id.to_string()),
            started_at,
            backup_ts,
            catalog_version,
            cluster_version,
            files,
            checksums,
            protected_timestamp: Some(self.protected_timestamp(
                &backup_id,
                parent.snapshot_ts,
                backup_ts,
            )),
            status: BackupStatus::Verifying,
        });

        self.put_manifest(&pending_manifest_key(&backup_id), &pending)
            .await?;
        self.verify_manifest_files(&pending).await?;

        let mut completed = pending;
        completed.completed_at = Some(Utc::now());
        completed.status = BackupStatus::Completed;
        self.put_final_manifest(&completed).await?;
        self.verify(&backup_id).await?;
        Ok(completed)
    }

    /// Verifies then returns the backed-up objects keyed by their logical name.
    /// For incremental backups, this recursively resolves the parent backup.
    pub fn restore<'a>(
        &'a self,
        backup_id: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<BackupObject>>> + Send + 'a>>
    {
        Box::pin(async move {
            self.verify(backup_id).await?;
            let manifest = self.load_manifest(backup_id).await?;

            if manifest.backup_type == BackupType::Incremental {
                if let Some(parent) = manifest.parent_backup_id.as_ref() {
                    let mut objects = self.restore(parent).await?;
                    objects.extend(self.restore_manifest_objects(&manifest).await?);
                    return Ok(objects);
                } else {
                    anyhow::bail!("Incremental backup {} missing parent_backup_id", backup_id);
                }
            }

            self.restore_manifest_objects(&manifest).await
        })
    }

    async fn restore_manifest_objects(
        &self,
        manifest: &BackupManifest,
    ) -> Result<Vec<BackupObject>> {
        let prefix = format!("{}/data/", manifest.backup_id);
        let mut out = Vec::new();
        for key in &manifest.files {
            let bytes = self.repo.get_object(key, None).await?;
            let name = key.strip_prefix(&prefix).unwrap_or(key).to_string();
            out.push(BackupObject { name, bytes });
        }
        Ok(out)
    }

    /// Builds a PITR restore plan without replaying WAL. The planner selects the
    /// newest completed backup at or before `target_ts`, verifies the backup and
    /// every selected archived WAL object, and rejects gaps in the indexed WAL
    /// segment sequence needed for replay.
    pub async fn plan_pitr_restore(&self, target_ts: u64) -> Result<PitrRestorePlan> {
        let manifests = self.completed_manifests().await?;
        let base = manifests
            .iter()
            .filter(|manifest| manifest.snapshot_ts <= target_ts)
            .max_by(|left, right| {
                left.snapshot_ts
                    .cmp(&right.snapshot_ts)
                    .then_with(|| left.backup_id.cmp(&right.backup_id))
            })
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no completed backup snapshot is at or before target_ts {target_ts}"
                )
            })?;

        self.verify(&base.backup_id).await?;
        let base_backup_chain = self.backup_chain_for_restore(&base).await?;
        let wal_segments = if target_ts == base.snapshot_ts {
            Vec::new()
        } else {
            self.plan_wal_segments(&base, target_ts).await?
        };

        Ok(PitrRestorePlan {
            plan_version: CURRENT_PITR_RESTORE_PLAN_VERSION,
            cluster_id: base.cluster_id,
            timeline_id: base.timeline_id,
            base_backup_id: base.backup_id,
            base_backup_chain,
            base_snapshot_ts: base.snapshot_ts,
            target_ts,
            replay_start_ts: base.snapshot_ts,
            replay_end_ts: target_ts,
            wal_segments,
            generated_at: Utc::now(),
        })
    }

    async fn backup_chain_for_restore(&self, manifest: &BackupManifest) -> Result<Vec<String>> {
        let mut chain = Vec::new();
        let mut current = manifest.clone();
        loop {
            chain.push(current.backup_id.clone());
            let Some(parent_id) = current.parent_backup_id.clone() else {
                break;
            };
            current = self.load_manifest(&parent_id).await?;
            if current.status != BackupStatus::Completed {
                anyhow::bail!("backup chain parent {parent_id} is not COMPLETE");
            }
        }
        chain.reverse();
        Ok(chain)
    }

    async fn plan_wal_segments(
        &self,
        base: &BackupManifest,
        target_ts: u64,
    ) -> Result<Vec<PitrRestoreWalSegment>> {
        let mut indexes = Vec::new();
        for index in self.load_wal_archive_indexes().await? {
            if index.index_version != CURRENT_WAL_ARCHIVE_INDEX_VERSION {
                anyhow::bail!(
                    "unsupported WAL archive index version {} for {}",
                    index.index_version,
                    index.index_object_key
                );
            }
            if index.timeline_id == base.timeline_id
                && index.upload_state == WalArchiveUploadState::Completed
            {
                indexes.push(index);
            }
        }

        let txn_ids_needed = indexes
            .iter()
            .flat_map(|index| index.committed_txns.iter())
            .filter(|txn| txn.commit_ts > base.snapshot_ts && txn.commit_ts <= target_ts)
            .map(|txn| txn.txn_id.clone())
            .collect::<HashSet<_>>();

        let mut indexes_by_sequence = BTreeMap::new();
        let mut needed_sequences = BTreeSet::new();
        for index in indexes {
            let has_needed_commit = index
                .committed_txns
                .iter()
                .any(|txn| txn.commit_ts > base.snapshot_ts && txn.commit_ts <= target_ts);
            let has_needed_txn_record = index
                .record_txn_ids
                .iter()
                .any(|txn_id| txn_ids_needed.contains(txn_id.as_str()));

            let sequence = wal_segment_sequence(&index.segment_id)?;
            if has_needed_commit || has_needed_txn_record {
                needed_sequences.insert(sequence);
            }
            if indexes_by_sequence.insert(sequence, index).is_some() {
                anyhow::bail!("duplicate WAL archive index for segment {sequence}.log");
            }
        }

        if needed_sequences.is_empty() {
            return Ok(Vec::new());
        }

        let first = *needed_sequences
            .iter()
            .next()
            .expect("needed_sequences is non-empty");
        let last = *needed_sequences
            .iter()
            .next_back()
            .expect("needed_sequences is non-empty");

        // Walk the segment lineage from `last` back to `first` via each segment's
        // recorded predecessor, rather than assuming a dense integer id range:
        // segment ids are sparse (the file-id counter is shared with SSTables and
        // compaction), so a contiguous-id check raised false "missing segment"
        // errors on perfectly intact archives. Every segment on the chain down to
        // `first` must be present; a break in the chain is a real, unrecoverable
        // gap and must fail rather than restore a partial window.
        let mut selected = BTreeMap::new();
        let mut current = last;
        loop {
            let index = indexes_by_sequence.remove(&current).ok_or_else(|| {
                anyhow::anyhow!(
                    "missing required WAL archive segment {current}.log while planning PITR from {} to {} (broken segment lineage)",
                    base.snapshot_ts,
                    target_ts
                )
            })?;
            let predecessor = index.predecessor;
            selected.insert(current, index);
            if current == first {
                break;
            }
            match predecessor {
                // Keep walking while the predecessor is still inside the window.
                Some(p) if p >= first => current = p,
                // The predecessor precedes the window (its commits are in the
                // base) or lineage wasn't recorded — every needed segment is in.
                _ => break,
            }
        }

        // Defense in depth: every segment carrying a needed commit must have been
        // reached by the lineage walk; a broken or pre-lineage chain that skipped
        // one means we cannot replay the full window consistently.
        for sequence in &needed_sequences {
            if !selected.contains_key(sequence) {
                anyhow::bail!(
                    "WAL segment {sequence}.log needed for PITR from {} to {} is not on the archived segment lineage",
                    base.snapshot_ts,
                    target_ts
                );
            }
        }

        let mut plan_segments = Vec::new();
        for (_, index) in selected {
            let bytes = self.repo.get_object(&index.wal_object_key, None).await?;
            if bytes.len() as u64 != index.size {
                anyhow::bail!(
                    "archived WAL object {} size mismatch: expected {}, got {}",
                    index.wal_object_key,
                    index.size,
                    bytes.len()
                );
            }
            let actual_checksum = checksum(&bytes);
            if actual_checksum != index.checksum {
                anyhow::bail!(
                    "archived WAL object {} checksum mismatch",
                    index.wal_object_key
                );
            }
            plan_segments.push(PitrRestoreWalSegment {
                segment_id: index.segment_id,
                wal_object_key: index.wal_object_key,
                index_object_key: index.index_object_key,
                first_commit_ts: index.first_commit_ts,
                last_commit_ts: index.last_commit_ts,
                checksum: index.checksum,
                size: index.size,
            });
        }

        Ok(plan_segments)
    }

    pub async fn load_pitr_wal_segments(
        &self,
        plan: &PitrRestorePlan,
    ) -> Result<Vec<PitrWalSegmentBytes>> {
        if plan.plan_version != CURRENT_PITR_RESTORE_PLAN_VERSION {
            anyhow::bail!(
                "unsupported PITR restore plan version {}",
                plan.plan_version
            );
        }

        let mut segments = Vec::new();
        for planned in &plan.wal_segments {
            let bytes = self.repo.get_object(&planned.wal_object_key, None).await?;
            if bytes.len() as u64 != planned.size {
                anyhow::bail!(
                    "archived WAL object {} size mismatch: expected {}, got {}",
                    planned.wal_object_key,
                    planned.size,
                    bytes.len()
                );
            }
            if checksum(&bytes) != planned.checksum {
                anyhow::bail!(
                    "archived WAL object {} checksum mismatch",
                    planned.wal_object_key
                );
            }
            segments.push(PitrWalSegmentBytes {
                segment_id: planned.segment_id.clone(),
                bytes,
            });
        }
        Ok(segments)
    }

    pub fn replay_pitr_wal_segments(
        plan: &PitrRestorePlan,
        segments: &[PitrWalSegmentBytes],
        kv: &dyn KvEngine,
        wal_key: Option<[u8; 32]>,
    ) -> Result<PitrReplayReport> {
        if plan.plan_version != CURRENT_PITR_RESTORE_PLAN_VERSION {
            anyhow::bail!(
                "unsupported PITR restore plan version {}",
                plan.plan_version
            );
        }
        if segments.len() != plan.wal_segments.len() {
            anyhow::bail!(
                "PITR replay segment count mismatch: plan has {}, loaded {}",
                plan.wal_segments.len(),
                segments.len()
            );
        }

        let mut report = PitrReplayReport::default();
        let mut active_txns = HashSet::new();
        for (planned, segment) in plan.wal_segments.iter().zip(segments.iter()) {
            if planned.segment_id != segment.segment_id {
                anyhow::bail!(
                    "PITR replay segment order mismatch: expected {}, got {}",
                    planned.segment_id,
                    segment.segment_id
                );
            }
            if segment.bytes.len() as u64 != planned.size {
                anyhow::bail!(
                    "archived WAL segment {} size mismatch during replay",
                    segment.segment_id
                );
            }
            if checksum(&segment.bytes) != planned.checksum {
                anyhow::bail!(
                    "archived WAL segment {} checksum mismatch during replay",
                    segment.segment_id
                );
            }

            let records = recover_wal_records(&segment.segment_id, &segment.bytes, wal_key)?;
            report.wal_segments_replayed += 1;
            for record in records {
                report.records_seen += 1;
                let WalRecord::V1(record) = record;
                match record {
                    WalRecordV1::BeginTxn { txn_id } => {
                        active_txns.insert(txn_id);
                    }
                    WalRecordV1::WriteIntent { txn_id, key, value } => {
                        kv.write_intent(txn_id, Bytes::from(key), Bytes::from(value))?;
                        active_txns.insert(txn_id);
                        report.writes_applied += 1;
                    }
                    WalRecordV1::DeleteIntent { txn_id, key } => {
                        kv.delete_intent(txn_id, Bytes::from(key))?;
                        active_txns.insert(txn_id);
                        report.deletes_applied += 1;
                    }
                    WalRecordV1::CommitTxn { txn_id, commit_ts } => {
                        if commit_ts <= plan.target_ts {
                            kv.commit(txn_id, commit_ts)?;
                            report.commits_applied += 1;
                        } else {
                            kv.abort(txn_id)?;
                            report.commits_skipped += 1;
                        }
                        active_txns.remove(&txn_id);
                    }
                    WalRecordV1::AbortTxn { txn_id } => {
                        kv.abort(txn_id)?;
                        active_txns.remove(&txn_id);
                        report.aborts_applied += 1;
                    }
                    // Lineage/metadata records carry no replayable mutation.
                    WalRecordV1::Checkpoint { .. } | WalRecordV1::SegmentHeader { .. } => {}
                }
            }
        }

        report.pending_txns_aborted = active_txns.len();
        for txn_id in active_txns {
            kv.abort(txn_id)?;
        }
        Ok(report)
    }

    pub fn restore_backup_objects_to_kv(
        objects: &[BackupObject],
        kv: &dyn KvEngine,
    ) -> Result<PitrReplayReport> {
        let mut report = PitrReplayReport {
            base_objects_restored: objects.len(),
            ..PitrReplayReport::default()
        };
        for obj in objects {
            if obj.name != "kv_data.json" {
                continue;
            }
            let dump = serde_json::from_slice::<Vec<serde_json::Value>>(&obj.bytes)?;
            for pair in dump {
                let Some(key) = json_bytes_field(&pair, "key") else {
                    continue;
                };
                let Some(version) = pair.get("version").and_then(|value| value.as_u64()) else {
                    continue;
                };
                let txn_id = nodus_storage_api::TxnId::new();
                if pair
                    .get("deleted")
                    .and_then(|deleted| deleted.as_bool())
                    .unwrap_or(false)
                {
                    kv.delete_intent(txn_id, key)?;
                } else if let Some(value) = json_bytes_field(&pair, "value") {
                    kv.write_intent(txn_id, key, value)?;
                } else {
                    continue;
                }
                kv.commit(txn_id, version)?;
                report.base_kv_versions_restored += 1;
            }
        }
        Ok(report)
    }

    pub fn merge_pitr_replay_reports(
        mut base: PitrReplayReport,
        wal: PitrReplayReport,
    ) -> PitrReplayReport {
        base.wal_segments_replayed += wal.wal_segments_replayed;
        base.records_seen += wal.records_seen;
        base.writes_applied += wal.writes_applied;
        base.deletes_applied += wal.deletes_applied;
        base.commits_applied += wal.commits_applied;
        base.commits_skipped += wal.commits_skipped;
        base.aborts_applied += wal.aborts_applied;
        base.pending_txns_aborted += wal.pending_txns_aborted;
        base
    }

    /// Deletes a backup's manifest and associated data files.
    pub async fn delete_backup(&self, backup_id: &str) -> Result<()> {
        for manifest in self.completed_manifests().await? {
            if manifest.parent_backup_id.as_deref() == Some(backup_id) {
                anyhow::bail!(
                    "cannot delete backup {backup_id}; retained backup {} depends on it",
                    manifest.backup_id
                );
            }
        }
        let prefix = format!("{backup_id}/");
        let objects = self.repo.list_objects(&prefix).await?;
        for obj in objects {
            self.repo.delete_object(&obj.key).await?;
        }
        Ok(())
    }

    /// Lists the ids of backups that have a manifest in the repository.
    pub async fn list_backups(&self) -> Result<Vec<String>> {
        let mut backups = Vec::new();
        for manifest in self.completed_manifests().await? {
            backups.push(manifest.backup_id);
        }
        Ok(backups)
    }

    async fn completed_manifests(&self) -> Result<Vec<BackupManifest>> {
        let objects = self.repo.list_objects("").await?;
        let mut manifests = Vec::new();
        for object in objects {
            if let Some(backup_id) = object.key.strip_suffix("/manifest.json")
                && let Ok(manifest) = self.load_manifest(backup_id).await
                && manifest.status == BackupStatus::Completed
            {
                manifests.push(manifest);
            }
        }
        Ok(manifests)
    }

    /// Returns the oldest backup snapshot that still protects MVCC history for
    /// future incrementals. Runtime GC must not advance beyond this timestamp.
    pub async fn protected_gc_watermark(&self) -> Result<Option<u64>> {
        Ok(self
            .completed_manifests()
            .await?
            .into_iter()
            .filter_map(|manifest| manifest.protected_timestamp.map(|_| manifest.snapshot_ts))
            .min())
    }

    /// Local WAL segment deletion is unsafe until WAL archive indexes can prove
    /// no retained restore point needs a segment. Archiving may continue, but
    /// cleanup should wait while any completed backup is retained.
    pub async fn wal_cleanup_allowed(&self) -> Result<bool> {
        Ok(self.completed_manifests().await?.is_empty())
    }

    pub async fn wal_segment_cleanup_allowed(&self, filename: &str) -> Result<bool> {
        let completed = self.completed_manifests().await?;
        if completed.is_empty() {
            return Ok(true);
        }
        let oldest_needed_ts = completed
            .iter()
            .filter_map(|manifest| {
                manifest
                    .protected_timestamp
                    .as_ref()
                    .map(|_| manifest.snapshot_ts)
            })
            .min()
            .ok_or_else(|| anyhow::anyhow!("retained backups have no protected timestamps"))?;
        let indexes = self.load_wal_archive_indexes().await?;
        let Some(index) = indexes.iter().find(|index| {
            index.segment_id == filename || index.wal_object_key == wal_key(filename)
        }) else {
            return Ok(false);
        };

        if index.upload_state != WalArchiveUploadState::Completed {
            return Ok(false);
        }
        if index
            .last_commit_ts
            .is_some_and(|last_commit_ts| last_commit_ts >= oldest_needed_ts)
        {
            return Ok(false);
        }

        let mut commits_needed_after_segment = std::collections::HashSet::new();
        for archived in &indexes {
            for committed in &archived.committed_txns {
                if committed.commit_ts >= oldest_needed_ts {
                    commits_needed_after_segment.insert(committed.txn_id.as_str());
                }
            }
        }

        Ok(!index
            .record_txn_ids
            .iter()
            .any(|txn_id| commits_needed_after_segment.contains(txn_id.as_str())))
    }

    /// Archives a WAL segment.
    pub async fn archive_wal(&self, filename: &str, data: Bytes) -> Result<()> {
        let key = wal_key(filename);
        self.repo
            .put_object(&key, data, PutOptions::default())
            .await
            .map(|_| ())
    }

    pub async fn archive_wal_indexed(
        &self,
        filename: &str,
        data: Bytes,
        record_txn_ids: Vec<String>,
        committed_txns: Vec<WalCommittedTxn>,
        predecessor: Option<u64>,
    ) -> Result<WalArchiveIndex> {
        let mut record_txn_ids = record_txn_ids;
        record_txn_ids.sort();
        record_txn_ids.dedup();

        let mut committed_txns = committed_txns;
        committed_txns.sort_by(|left, right| {
            left.commit_ts
                .cmp(&right.commit_ts)
                .then_with(|| left.txn_id.cmp(&right.txn_id))
        });
        committed_txns.dedup();

        let first_commit_ts = committed_txns.iter().map(|txn| txn.commit_ts).min();
        let last_commit_ts = committed_txns.iter().map(|txn| txn.commit_ts).max();
        let wal_object_key = wal_key(filename);
        let index_object_key = wal_index_key(filename);
        let index = WalArchiveIndex {
            index_version: CURRENT_WAL_ARCHIVE_INDEX_VERSION,
            timeline_id: DEFAULT_TIMELINE_ID.to_string(),
            segment_id: filename.to_string(),
            wal_object_key: wal_object_key.clone(),
            index_object_key: index_object_key.clone(),
            size: data.len() as u64,
            checksum: checksum(&data),
            first_commit_ts,
            last_commit_ts,
            record_txn_ids,
            committed_txns,
            predecessor,
            archived_at: Utc::now(),
            upload_state: WalArchiveUploadState::Completed,
        };

        self.repo
            .put_object(&wal_object_key, data, PutOptions::default())
            .await?;
        self.put_wal_archive_index(&index).await?;
        Ok(index)
    }

    async fn put_wal_archive_index(&self, index: &WalArchiveIndex) -> Result<()> {
        let body = serde_json::to_vec(index)?;
        self.repo
            .put_object(
                &index.index_object_key,
                Bytes::from(body),
                PutOptions {
                    content_type: Some("application/json".into()),
                },
            )
            .await
            .map(|_| ())
    }

    pub async fn load_wal_archive_indexes(&self) -> Result<Vec<WalArchiveIndex>> {
        let objects = self.repo.list_objects("wal_archive/").await?;
        let mut indexes = Vec::new();
        for object in objects {
            if !object.key.ends_with(".index.json") {
                continue;
            }
            let bytes = self.repo.get_object(&object.key, None).await?;
            let index = serde_json::from_slice::<WalArchiveIndex>(&bytes)?;
            indexes.push(index);
        }
        indexes.sort_by(|left, right| left.segment_id.cmp(&right.segment_id));
        Ok(indexes)
    }

    /// Retrieves all archived WAL segments, sorted chronologically by numeric file stem.
    pub async fn get_archived_wals(&self) -> Result<Vec<(String, Bytes)>> {
        let objects = self.repo.list_objects("wal_archive/").await?;
        let mut wals = Vec::new();
        for obj in objects {
            if !obj.key.ends_with(".log") {
                continue;
            }
            let bytes = self.repo.get_object(&obj.key, None).await?;
            let name = obj
                .key
                .strip_prefix("wal_archive/")
                .unwrap_or(&obj.key)
                .to_string();
            wals.push((name, bytes));
        }
        wals.sort_by_key(|(name, _)| {
            name.strip_suffix(".log")
                .unwrap_or("0")
                .parse::<u64>()
                .unwrap_or(0)
        });
        Ok(wals)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodus_storage_api::{KvEngine, TxnId};
    use nodus_storage_mem::MemKvEngine;
    use nodus_storage_wal::{WalRecord, WalRecordV1};

    fn test_wal_bytes(records: Vec<WalRecord>) -> Bytes {
        let path = std::env::temp_dir().join(format!("nodus-backup-test-wal-{}", Uuid::new_v4()));
        let wal = FileWalEngine::new(&path).unwrap();
        for record in records {
            wal.append(record).unwrap();
        }
        wal.sync().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        Bytes::from(bytes)
    }

    #[tokio::test]
    async fn test_mem_repository() {
        let repo = MemBackupRepository::new();
        repo.put_object("test.txt", Bytes::from("hello"), PutOptions::default())
            .await
            .unwrap();

        assert!(repo.object_exists("test.txt").await.unwrap());
        let content = repo.get_object("test.txt", None).await.unwrap();
        assert_eq!(content, Bytes::from("hello"));

        let list = repo.list_objects("test").await.unwrap();
        assert_eq!(list.len(), 1);

        repo.delete_object("test.txt").await.unwrap();
        assert!(!repo.object_exists("test.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_fs_repository_roundtrip() {
        let dir = std::env::temp_dir().join(format!("nodus_bk_{}", Uuid::new_v4()));
        let repo = FsBackupRepository::new(&dir);
        repo.put_object("a/b.txt", Bytes::from("data"), PutOptions::default())
            .await
            .unwrap();
        assert!(repo.object_exists("a/b.txt").await.unwrap());
        assert_eq!(
            repo.get_object("a/b.txt", None).await.unwrap(),
            Bytes::from("data")
        );
        let listed = repo.list_objects("a/").await.unwrap();
        assert_eq!(listed.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_backup_restore_and_verify() {
        let dir = std::env::temp_dir().join(format!("nodus_bk_{}", Uuid::new_v4()));
        let repo: Arc<dyn BackupRepository> = Arc::new(FsBackupRepository::new(&dir));
        let orch = BackupOrchestrator::new(repo.clone());

        let manifest = orch
            .create_full_backup(
                "cluster-1",
                42,
                7,
                3,
                vec![
                    BackupObject {
                        name: "catalog".into(),
                        bytes: Bytes::from("cat"),
                    },
                    BackupObject {
                        name: "shard-0".into(),
                        bytes: Bytes::from("rows"),
                    },
                ],
            )
            .await
            .unwrap();
        assert_eq!(manifest.status, BackupStatus::Completed);
        assert_eq!(manifest.manifest_version, CURRENT_MANIFEST_VERSION);
        assert_eq!(
            manifest.repository_layout_version,
            CURRENT_REPOSITORY_LAYOUT_VERSION
        );
        assert_eq!(
            manifest.object_format_version,
            CURRENT_OBJECT_FORMAT_VERSION
        );
        assert_eq!(manifest.timeline_id, DEFAULT_TIMELINE_ID);
        assert_eq!(manifest.snapshot_ts, 42);
        assert!(manifest.wal_start.is_none());
        assert!(manifest.wal_end.is_none());
        assert!(manifest.compression.is_none());
        assert!(manifest.encryption.is_none());
        assert!(manifest.restore_validation.is_none());
        assert_eq!(
            manifest
                .protected_timestamp
                .as_ref()
                .map(|p| p.upper_bound_ts),
            Some(42)
        );
        assert_eq!(
            manifest.repository_capabilities,
            RepositoryCapabilities::default()
        );
        assert!(
            repo.object_exists(&pending_manifest_key(&manifest.backup_id))
                .await
                .unwrap()
        );

        // A freshly written backup verifies and restores its objects.
        orch.verify(&manifest.backup_id).await.unwrap();
        let restored = orch.restore(&manifest.backup_id).await.unwrap();
        assert_eq!(restored.len(), 2);

        assert_eq!(
            orch.list_backups().await.unwrap(),
            vec![manifest.backup_id.clone()]
        );

        // Corrupting a file is detected by verify.
        let key = data_key(&manifest.backup_id, "shard-0");
        repo.put_object(&key, Bytes::from("tampered"), PutOptions::default())
            .await
            .unwrap();
        assert!(orch.verify(&manifest.backup_id).await.is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_failed_backup_manifest() {
        let dir = std::env::temp_dir().join(format!("nodus_bk_{}", Uuid::new_v4()));
        let repo: Arc<dyn BackupRepository> = Arc::new(FsBackupRepository::new(&dir));
        let orch = BackupOrchestrator::new(repo.clone());

        let mut checksums = HashMap::new();
        checksums.insert("f1".to_string(), "abc".to_string());
        let manifest = BackupManifest {
            manifest_version: CURRENT_MANIFEST_VERSION,
            repository_layout_version: CURRENT_REPOSITORY_LAYOUT_VERSION,
            object_format_version: CURRENT_OBJECT_FORMAT_VERSION,
            backup_id: "test-id-123".to_string(),
            cluster_id: "c".to_string(),
            timeline_id: DEFAULT_TIMELINE_ID.to_string(),
            backup_type: BackupType::Full,
            parent_backup_id: None,
            started_at: Utc::now(),
            completed_at: None,
            snapshot_ts: 0,
            backup_ts: 0,
            catalog_version: 0,
            cluster_version: 0,
            wal_start: None,
            wal_end: None,
            files: vec!["f1".to_string()],
            checksums,
            compression: None,
            encryption: None,
            restore_validation: None,
            protected_timestamp: None,
            repository_capabilities: RepositoryCapabilities::default(),
            status: BackupStatus::Failed,
        };

        let body = serde_json::to_vec(&manifest).unwrap();
        repo.put_object(
            &manifest_key("test-id-123"),
            Bytes::from(body),
            PutOptions::default(),
        )
        .await
        .unwrap();
        repo.put_object("f1", Bytes::from("bad"), PutOptions::default())
            .await
            .unwrap();

        let m = orch.load_manifest("test-id-123").await.unwrap();
        assert_eq!(m.status, BackupStatus::Failed);
        assert!(orch.verify("test-id-123").await.is_err());
        assert!(orch.list_backups().await.unwrap().is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_pending_manifest_is_not_restorable_or_listed() {
        let dir = std::env::temp_dir().join(format!("nodus_bk_{}", Uuid::new_v4()));
        let repo: Arc<dyn BackupRepository> = Arc::new(FsBackupRepository::new(&dir));
        let orch = BackupOrchestrator::new(repo.clone());
        let backup_id = "pending-only";
        let manifest = BackupManifest {
            manifest_version: CURRENT_MANIFEST_VERSION,
            repository_layout_version: CURRENT_REPOSITORY_LAYOUT_VERSION,
            object_format_version: CURRENT_OBJECT_FORMAT_VERSION,
            backup_id: backup_id.to_string(),
            cluster_id: "c".to_string(),
            timeline_id: DEFAULT_TIMELINE_ID.to_string(),
            backup_type: BackupType::Full,
            parent_backup_id: None,
            started_at: Utc::now(),
            completed_at: None,
            snapshot_ts: 1,
            backup_ts: 1,
            catalog_version: 1,
            cluster_version: 1,
            wal_start: None,
            wal_end: None,
            files: Vec::new(),
            checksums: HashMap::new(),
            compression: None,
            encryption: None,
            restore_validation: None,
            protected_timestamp: None,
            repository_capabilities: RepositoryCapabilities::default(),
            status: BackupStatus::Verifying,
        };

        repo.put_object(
            &pending_manifest_key(backup_id),
            Bytes::from(serde_json::to_vec(&manifest).unwrap()),
            PutOptions::default(),
        )
        .await
        .unwrap();

        assert!(orch.list_backups().await.unwrap().is_empty());
        assert!(orch.verify(backup_id).await.is_err());
        assert!(orch.restore(backup_id).await.is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_final_manifest_is_append_only_for_orchestrator() {
        let repo: Arc<dyn BackupRepository> = Arc::new(MemBackupRepository::new());
        let orch = BackupOrchestrator::new(repo);
        let backup_id = "append-only";
        let manifest = BackupManifest {
            manifest_version: CURRENT_MANIFEST_VERSION,
            repository_layout_version: CURRENT_REPOSITORY_LAYOUT_VERSION,
            object_format_version: CURRENT_OBJECT_FORMAT_VERSION,
            backup_id: backup_id.to_string(),
            cluster_id: "c".to_string(),
            timeline_id: DEFAULT_TIMELINE_ID.to_string(),
            backup_type: BackupType::Full,
            parent_backup_id: None,
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            snapshot_ts: 1,
            backup_ts: 1,
            catalog_version: 1,
            cluster_version: 1,
            wal_start: None,
            wal_end: None,
            files: Vec::new(),
            checksums: HashMap::new(),
            compression: None,
            encryption: None,
            restore_validation: None,
            protected_timestamp: None,
            repository_capabilities: RepositoryCapabilities::default(),
            status: BackupStatus::Completed,
        };

        orch.put_final_manifest(&manifest).await.unwrap();
        assert!(orch.put_final_manifest(&manifest).await.is_err());
    }

    #[tokio::test]
    async fn test_incremental_backup_restores_after_parent() {
        let repo: Arc<dyn BackupRepository> = Arc::new(MemBackupRepository::new());
        let orch = BackupOrchestrator::new(repo);

        let full = orch
            .create_full_backup(
                "cluster-1",
                10,
                1,
                1,
                vec![BackupObject {
                    name: "kv_data.json".into(),
                    bytes: Bytes::from("full"),
                }],
            )
            .await
            .unwrap();
        let incremental = orch
            .create_incremental_backup(
                "cluster-1",
                &full.backup_id,
                20,
                1,
                1,
                vec![BackupObject {
                    name: "kv_data.json".into(),
                    bytes: Bytes::from("incremental"),
                }],
            )
            .await
            .unwrap();

        assert_eq!(incremental.backup_type, BackupType::Incremental);
        assert_eq!(incremental.parent_backup_id, Some(full.backup_id));
        assert_eq!(
            incremental
                .protected_timestamp
                .as_ref()
                .map(|p| (p.lower_bound_ts, p.upper_bound_ts)),
            Some((10, 20))
        );

        let restored = orch.restore(&incremental.backup_id).await.unwrap();
        assert_eq!(restored.len(), 2);
        assert_eq!(restored[0].bytes, Bytes::from("full"));
        assert_eq!(restored[1].bytes, Bytes::from("incremental"));
    }

    #[tokio::test]
    async fn test_incremental_backup_requires_changes_and_newer_timestamp() {
        let repo: Arc<dyn BackupRepository> = Arc::new(MemBackupRepository::new());
        let orch = BackupOrchestrator::new(repo);
        let full = orch
            .create_full_backup(
                "cluster-1",
                10,
                1,
                1,
                vec![BackupObject {
                    name: "kv_data.json".into(),
                    bytes: Bytes::from("full"),
                }],
            )
            .await
            .unwrap();

        assert!(
            orch.create_incremental_backup("cluster-1", &full.backup_id, 11, 1, 1, vec![])
                .await
                .is_err()
        );
        assert!(
            orch.create_incremental_backup(
                "cluster-1",
                &full.backup_id,
                10,
                1,
                1,
                vec![BackupObject {
                    name: "kv_data.json".into(),
                    bytes: Bytes::from("same-ts"),
                }],
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_retained_backups_protect_gc_and_wal_cleanup() {
        let repo: Arc<dyn BackupRepository> = Arc::new(MemBackupRepository::new());
        let orch = BackupOrchestrator::new(repo);

        assert_eq!(orch.protected_gc_watermark().await.unwrap(), None);
        assert!(orch.wal_cleanup_allowed().await.unwrap());

        let full = orch
            .create_full_backup(
                "cluster-1",
                10,
                1,
                1,
                vec![BackupObject {
                    name: "kv_data.json".into(),
                    bytes: Bytes::from("full"),
                }],
            )
            .await
            .unwrap();
        let incremental = orch
            .create_incremental_backup(
                "cluster-1",
                &full.backup_id,
                20,
                1,
                1,
                vec![BackupObject {
                    name: "kv_data.json".into(),
                    bytes: Bytes::from("incremental"),
                }],
            )
            .await
            .unwrap();

        assert_eq!(orch.protected_gc_watermark().await.unwrap(), Some(10));
        assert!(!orch.wal_cleanup_allowed().await.unwrap());

        assert!(orch.delete_backup(&full.backup_id).await.is_err());
        assert_eq!(orch.protected_gc_watermark().await.unwrap(), Some(10));
        assert!(!orch.wal_cleanup_allowed().await.unwrap());

        orch.delete_backup(&incremental.backup_id).await.unwrap();
        assert_eq!(orch.protected_gc_watermark().await.unwrap(), Some(10));
        assert!(!orch.wal_cleanup_allowed().await.unwrap());

        orch.delete_backup(&full.backup_id).await.unwrap();
        assert_eq!(orch.protected_gc_watermark().await.unwrap(), None);
        assert!(orch.wal_cleanup_allowed().await.unwrap());
    }

    #[tokio::test]
    async fn test_wal_archive_index_drives_segment_cleanup() {
        let repo: Arc<dyn BackupRepository> = Arc::new(MemBackupRepository::new());
        let orch = BackupOrchestrator::new(repo);

        assert!(
            orch.archive_wal_indexed(
                "0.log",
                Bytes::from("old"),
                vec!["old-txn".into()],
                vec![WalCommittedTxn {
                    txn_id: "old-txn".into(),
                    commit_ts: 5,
                }],
                None,
            )
            .await
            .is_ok()
        );
        assert!(orch.wal_segment_cleanup_allowed("0.log").await.unwrap());

        let full = orch
            .create_full_backup(
                "cluster-1",
                10,
                1,
                1,
                vec![BackupObject {
                    name: "kv_data.json".into(),
                    bytes: Bytes::from("full"),
                }],
            )
            .await
            .unwrap();

        orch.archive_wal_indexed(
            "1.log",
            Bytes::from("before"),
            vec!["before-txn".into()],
            vec![WalCommittedTxn {
                txn_id: "before-txn".into(),
                commit_ts: 5,
            }],
            Some(0),
        )
        .await
        .unwrap();
        orch.archive_wal_indexed(
            "2.log",
            Bytes::from("after"),
            vec!["after-txn".into()],
            vec![WalCommittedTxn {
                txn_id: "after-txn".into(),
                commit_ts: 12,
            }],
            Some(1),
        )
        .await
        .unwrap();
        orch.archive_wal_indexed(
            "3.log",
            Bytes::from("write-before-commit"),
            vec!["late-txn".into()],
            vec![],
            Some(2),
        )
        .await
        .unwrap();
        orch.archive_wal_indexed(
            "4.log",
            Bytes::from("commit-after"),
            vec![],
            vec![WalCommittedTxn {
                txn_id: "late-txn".into(),
                commit_ts: 15,
            }],
            Some(3),
        )
        .await
        .unwrap();

        assert!(orch.wal_segment_cleanup_allowed("1.log").await.unwrap());
        assert!(!orch.wal_segment_cleanup_allowed("2.log").await.unwrap());
        assert!(!orch.wal_segment_cleanup_allowed("3.log").await.unwrap());
        assert!(!orch.wal_segment_cleanup_allowed("4.log").await.unwrap());
        assert_eq!(orch.load_wal_archive_indexes().await.unwrap().len(), 5);
        assert_eq!(orch.get_archived_wals().await.unwrap().len(), 5);

        orch.delete_backup(&full.backup_id).await.unwrap();
        assert!(orch.wal_segment_cleanup_allowed("2.log").await.unwrap());
        assert!(orch.wal_segment_cleanup_allowed("3.log").await.unwrap());
        assert!(orch.wal_segment_cleanup_allowed("4.log").await.unwrap());
    }

    #[tokio::test]
    async fn test_pitr_restore_plan_selects_latest_base_and_wal_range() {
        let repo: Arc<dyn BackupRepository> = Arc::new(MemBackupRepository::new());
        let orch = BackupOrchestrator::new(repo);

        let full = orch
            .create_full_backup(
                "cluster-1",
                10,
                1,
                1,
                vec![BackupObject {
                    name: "kv_data.json".into(),
                    bytes: Bytes::from("full"),
                }],
            )
            .await
            .unwrap();
        let incremental = orch
            .create_incremental_backup(
                "cluster-1",
                &full.backup_id,
                20,
                1,
                1,
                vec![BackupObject {
                    name: "kv_data.json".into(),
                    bytes: Bytes::from("incremental"),
                }],
            )
            .await
            .unwrap();

        orch.archive_wal_indexed(
            "1.log",
            Bytes::from("before-base"),
            vec!["before-base".into()],
            vec![WalCommittedTxn {
                txn_id: "before-base".into(),
                commit_ts: 12,
            }],
            Some(0),
        )
        .await
        .unwrap();
        orch.archive_wal_indexed(
            "2.log",
            Bytes::from("after-base"),
            vec!["after-base".into()],
            vec![WalCommittedTxn {
                txn_id: "after-base".into(),
                commit_ts: 22,
            }],
            Some(1),
        )
        .await
        .unwrap();
        orch.archive_wal_indexed(
            "3.log",
            Bytes::from("late-write"),
            vec!["late-txn".into()],
            vec![],
            Some(2),
        )
        .await
        .unwrap();
        orch.archive_wal_indexed(
            "4.log",
            Bytes::from("late-commit"),
            vec![],
            vec![WalCommittedTxn {
                txn_id: "late-txn".into(),
                commit_ts: 24,
            }],
            Some(3),
        )
        .await
        .unwrap();

        let plan = orch.plan_pitr_restore(25).await.unwrap();
        assert_eq!(plan.plan_version, CURRENT_PITR_RESTORE_PLAN_VERSION);
        assert_eq!(plan.base_backup_id, incremental.backup_id);
        assert_eq!(
            plan.base_backup_chain,
            vec![full.backup_id, incremental.backup_id]
        );
        assert_eq!(plan.base_snapshot_ts, 20);
        assert_eq!(plan.target_ts, 25);
        assert_eq!(
            plan.wal_segments
                .iter()
                .map(|segment| segment.segment_id.as_str())
                .collect::<Vec<_>>(),
            vec!["2.log", "3.log", "4.log"]
        );
    }

    #[tokio::test]
    async fn test_pitr_restore_plan_allows_snapshot_target_without_wal() {
        let repo: Arc<dyn BackupRepository> = Arc::new(MemBackupRepository::new());
        let orch = BackupOrchestrator::new(repo);
        let full = orch
            .create_full_backup(
                "cluster-1",
                10,
                1,
                1,
                vec![BackupObject {
                    name: "kv_data.json".into(),
                    bytes: Bytes::from("full"),
                }],
            )
            .await
            .unwrap();

        let plan = orch.plan_pitr_restore(10).await.unwrap();
        assert_eq!(plan.base_backup_id, full.backup_id);
        assert!(plan.wal_segments.is_empty());
    }

    #[tokio::test]
    async fn test_pitr_restore_plan_rejects_missing_required_wal_segment() {
        let repo: Arc<dyn BackupRepository> = Arc::new(MemBackupRepository::new());
        let orch = BackupOrchestrator::new(repo);
        orch.create_full_backup(
            "cluster-1",
            10,
            1,
            1,
            vec![BackupObject {
                name: "kv_data.json".into(),
                bytes: Bytes::from("full"),
            }],
        )
        .await
        .unwrap();

        orch.archive_wal_indexed(
            "1.log",
            Bytes::from("first"),
            vec!["first".into()],
            vec![WalCommittedTxn {
                txn_id: "first".into(),
                commit_ts: 12,
            }],
            Some(0),
        )
        .await
        .unwrap();
        orch.archive_wal_indexed(
            "3.log",
            Bytes::from("third"),
            vec!["third".into()],
            vec![WalCommittedTxn {
                txn_id: "third".into(),
                commit_ts: 14,
            }],
            Some(2),
        )
        .await
        .unwrap();

        let err = orch.plan_pitr_restore(15).await.unwrap_err();
        assert!(
            err.to_string().contains("broken segment lineage"),
            "expected a lineage-gap error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_pitr_restore_plan_includes_intermediate_indexed_segments() {
        let repo: Arc<dyn BackupRepository> = Arc::new(MemBackupRepository::new());
        let orch = BackupOrchestrator::new(repo);
        orch.create_full_backup(
            "cluster-1",
            10,
            1,
            1,
            vec![BackupObject {
                name: "kv_data.json".into(),
                bytes: Bytes::from("full"),
            }],
        )
        .await
        .unwrap();

        orch.archive_wal_indexed(
            "1.log",
            Bytes::from("first"),
            vec!["first".into()],
            vec![WalCommittedTxn {
                txn_id: "first".into(),
                commit_ts: 12,
            }],
            Some(0),
        )
        .await
        .unwrap();
        orch.archive_wal_indexed("2.log", Bytes::from("checkpoint"), vec![], vec![], Some(1))
            .await
            .unwrap();
        orch.archive_wal_indexed(
            "3.log",
            Bytes::from("third"),
            vec!["third".into()],
            vec![WalCommittedTxn {
                txn_id: "third".into(),
                commit_ts: 14,
            }],
            Some(2),
        )
        .await
        .unwrap();

        let plan = orch.plan_pitr_restore(15).await.unwrap();
        assert_eq!(
            plan.wal_segments
                .iter()
                .map(|segment| segment.segment_id.as_str())
                .collect::<Vec<_>>(),
            vec!["1.log", "2.log", "3.log"]
        );
    }

    /// WAL-segment ids are sparse (the file-id counter is shared with SSTables
    /// and compaction), so the planner must follow the recorded lineage rather
    /// than require every integer id between first and last — which would have
    /// failed this intact archive with a false "missing segment".
    #[tokio::test]
    async fn test_pitr_restore_plan_accepts_sparse_segment_ids() {
        let repo: Arc<dyn BackupRepository> = Arc::new(MemBackupRepository::new());
        let orch = BackupOrchestrator::new(repo);
        orch.create_full_backup(
            "cluster-1",
            10,
            1,
            1,
            vec![BackupObject {
                name: "kv_data.json".into(),
                bytes: Bytes::from("full"),
            }],
        )
        .await
        .unwrap();

        // Segments 2 → 6 → 9: ids 3,4,5,7,8 were SSTable/compaction files, never
        // WAL segments. The lineage links make this an unbroken chain.
        orch.archive_wal_indexed(
            "2.log",
            Bytes::from("a"),
            vec!["a".into()],
            vec![WalCommittedTxn {
                txn_id: "a".into(),
                commit_ts: 12,
            }],
            Some(1),
        )
        .await
        .unwrap();
        orch.archive_wal_indexed(
            "6.log",
            Bytes::from("b"),
            vec!["b".into()],
            vec![WalCommittedTxn {
                txn_id: "b".into(),
                commit_ts: 13,
            }],
            Some(2),
        )
        .await
        .unwrap();
        orch.archive_wal_indexed(
            "9.log",
            Bytes::from("c"),
            vec!["c".into()],
            vec![WalCommittedTxn {
                txn_id: "c".into(),
                commit_ts: 14,
            }],
            Some(6),
        )
        .await
        .unwrap();

        let plan = orch.plan_pitr_restore(15).await.unwrap();
        assert_eq!(
            plan.wal_segments
                .iter()
                .map(|segment| segment.segment_id.as_str())
                .collect::<Vec<_>>(),
            vec!["2.log", "6.log", "9.log"],
            "intact sparse-id chain must not be rejected as a gap"
        );
    }

    #[tokio::test]
    async fn test_pitr_restore_plan_rejects_corrupt_archived_wal_object() {
        let repo: Arc<dyn BackupRepository> = Arc::new(MemBackupRepository::new());
        let orch = BackupOrchestrator::new(repo.clone());
        orch.create_full_backup(
            "cluster-1",
            10,
            1,
            1,
            vec![BackupObject {
                name: "kv_data.json".into(),
                bytes: Bytes::from("full"),
            }],
        )
        .await
        .unwrap();
        let index = orch
            .archive_wal_indexed(
                "1.log",
                Bytes::from("original"),
                vec!["txn".into()],
                vec![WalCommittedTxn {
                    txn_id: "txn".into(),
                    commit_ts: 12,
                }],
                Some(0),
            )
            .await
            .unwrap();

        repo.put_object(
            &index.wal_object_key,
            Bytes::from("tampered"),
            PutOptions::default(),
        )
        .await
        .unwrap();

        let err = orch.plan_pitr_restore(12).await.unwrap_err();
        assert!(err.to_string().contains("checksum mismatch"));
    }

    #[tokio::test]
    async fn test_pitr_replay_restores_base_and_replays_to_target_ts() {
        let repo: Arc<dyn BackupRepository> = Arc::new(MemBackupRepository::new());
        let orch = BackupOrchestrator::new(repo);
        let base_dump = serde_json::json!([
            {
                "key": [98],
                "value": [48],
                "deleted": false,
                "version": 10
            }
        ]);
        orch.create_full_backup(
            "cluster-1",
            10,
            1,
            1,
            vec![BackupObject {
                name: "kv_data.json".into(),
                bytes: Bytes::from(serde_json::to_vec(&base_dump).unwrap()),
            }],
        )
        .await
        .unwrap();

        let committed = TxnId::new();
        let exact = TxnId::new();
        let late = TxnId::new();
        let pending = TxnId::new();
        let wal = test_wal_bytes(vec![
            WalRecord::V1(WalRecordV1::BeginTxn { txn_id: committed }),
            WalRecord::V1(WalRecordV1::WriteIntent {
                txn_id: committed,
                key: b"a".to_vec(),
                value: b"15".to_vec(),
            }),
            WalRecord::V1(WalRecordV1::CommitTxn {
                txn_id: committed,
                commit_ts: 15,
            }),
            WalRecord::V1(WalRecordV1::BeginTxn { txn_id: exact }),
            WalRecord::V1(WalRecordV1::WriteIntent {
                txn_id: exact,
                key: b"e".to_vec(),
                value: b"20".to_vec(),
            }),
            WalRecord::V1(WalRecordV1::CommitTxn {
                txn_id: exact,
                commit_ts: 20,
            }),
            WalRecord::V1(WalRecordV1::BeginTxn { txn_id: late }),
            WalRecord::V1(WalRecordV1::WriteIntent {
                txn_id: late,
                key: b"l".to_vec(),
                value: b"25".to_vec(),
            }),
            WalRecord::V1(WalRecordV1::CommitTxn {
                txn_id: late,
                commit_ts: 25,
            }),
            WalRecord::V1(WalRecordV1::BeginTxn { txn_id: pending }),
            WalRecord::V1(WalRecordV1::WriteIntent {
                txn_id: pending,
                key: b"p".to_vec(),
                value: b"pending".to_vec(),
            }),
        ]);
        orch.archive_wal_indexed(
            "1.log",
            wal,
            vec![
                committed.0.to_string(),
                exact.0.to_string(),
                late.0.to_string(),
                pending.0.to_string(),
            ],
            vec![
                WalCommittedTxn {
                    txn_id: committed.0.to_string(),
                    commit_ts: 15,
                },
                WalCommittedTxn {
                    txn_id: exact.0.to_string(),
                    commit_ts: 20,
                },
                WalCommittedTxn {
                    txn_id: late.0.to_string(),
                    commit_ts: 25,
                },
            ],
            Some(0),
        )
        .await
        .unwrap();

        let plan = orch.plan_pitr_restore(20).await.unwrap();
        let objects = orch.restore(&plan.base_backup_id).await.unwrap();
        let wal_segments = orch.load_pitr_wal_segments(&plan).await.unwrap();
        let kv = MemKvEngine::new();
        let base_report = BackupOrchestrator::restore_backup_objects_to_kv(&objects, &kv).unwrap();
        let wal_report =
            BackupOrchestrator::replay_pitr_wal_segments(&plan, &wal_segments, &kv, None).unwrap();
        let report = BackupOrchestrator::merge_pitr_replay_reports(base_report, wal_report);

        assert_eq!(kv.get(b"b", 20).unwrap(), Some(Bytes::from_static(b"0")));
        assert_eq!(kv.get(b"a", 20).unwrap(), Some(Bytes::from_static(b"15")));
        assert_eq!(kv.get(b"e", 20).unwrap(), Some(Bytes::from_static(b"20")));
        assert_eq!(kv.get(b"l", 30).unwrap(), None);
        assert_eq!(kv.get(b"p", 30).unwrap(), None);
        assert_eq!(report.base_kv_versions_restored, 1);
        assert_eq!(report.wal_segments_replayed, 1);
        assert_eq!(report.commits_applied, 2);
        assert_eq!(report.commits_skipped, 1);
        assert_eq!(report.pending_txns_aborted, 1);
    }

    #[tokio::test]
    async fn test_pitr_replay_preserves_aborts_and_delete_tombstones() {
        let repo: Arc<dyn BackupRepository> = Arc::new(MemBackupRepository::new());
        let orch = BackupOrchestrator::new(repo);
        orch.create_full_backup(
            "cluster-1",
            10,
            1,
            1,
            vec![BackupObject {
                name: "kv_data.json".into(),
                bytes: Bytes::from("[]"),
            }],
        )
        .await
        .unwrap();

        let put = TxnId::new();
        let delete = TxnId::new();
        let aborted = TxnId::new();
        let wal = test_wal_bytes(vec![
            WalRecord::V1(WalRecordV1::WriteIntent {
                txn_id: put,
                key: b"d".to_vec(),
                value: b"visible-before-delete".to_vec(),
            }),
            WalRecord::V1(WalRecordV1::CommitTxn {
                txn_id: put,
                commit_ts: 12,
            }),
            WalRecord::V1(WalRecordV1::DeleteIntent {
                txn_id: delete,
                key: b"d".to_vec(),
            }),
            WalRecord::V1(WalRecordV1::CommitTxn {
                txn_id: delete,
                commit_ts: 18,
            }),
            WalRecord::V1(WalRecordV1::WriteIntent {
                txn_id: aborted,
                key: b"x".to_vec(),
                value: b"aborted".to_vec(),
            }),
            WalRecord::V1(WalRecordV1::AbortTxn { txn_id: aborted }),
        ]);
        orch.archive_wal_indexed(
            "1.log",
            wal,
            vec![
                put.0.to_string(),
                delete.0.to_string(),
                aborted.0.to_string(),
            ],
            vec![
                WalCommittedTxn {
                    txn_id: put.0.to_string(),
                    commit_ts: 12,
                },
                WalCommittedTxn {
                    txn_id: delete.0.to_string(),
                    commit_ts: 18,
                },
            ],
            Some(0),
        )
        .await
        .unwrap();

        let plan = orch.plan_pitr_restore(20).await.unwrap();
        let wal_segments = orch.load_pitr_wal_segments(&plan).await.unwrap();
        let kv = MemKvEngine::new();
        let report =
            BackupOrchestrator::replay_pitr_wal_segments(&plan, &wal_segments, &kv, None).unwrap();

        assert_eq!(
            kv.get(b"d", 17).unwrap(),
            Some(Bytes::from_static(b"visible-before-delete"))
        );
        assert_eq!(kv.get(b"d", 20).unwrap(), None);
        assert_eq!(kv.get(b"x", 20).unwrap(), None);
        assert_eq!(report.deletes_applied, 1);
        assert_eq!(report.aborts_applied, 1);
    }
}
