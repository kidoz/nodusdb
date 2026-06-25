mod s3;
pub use s3::{S3BackupRepository, S3Config};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
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
use std::collections::HashMap;
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
const DEFAULT_TIMELINE_ID: &str = "default";

fn checksum(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
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

    /// Archives a WAL segment.
    pub async fn archive_wal(&self, filename: &str, data: Bytes) -> Result<()> {
        let key = format!("wal_archive/{}", filename);
        self.repo
            .put_object(&key, data, PutOptions::default())
            .await
            .map(|_| ())
    }

    /// Retrieves all archived WAL segments, sorted chronologically by numeric file stem.
    pub async fn get_archived_wals(&self) -> Result<Vec<(String, Bytes)>> {
        let objects = self.repo.list_objects("wal_archive/").await?;
        let mut wals = Vec::new();
        for obj in objects {
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
}
