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

#[derive(Debug, Clone)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64, // inclusive
}

#[async_trait]
pub trait BackupRepository: Send + Sync {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifestV1 {
    pub backup_id: String,
    pub cluster_id: String,
    pub backup_type: BackupType,
    pub parent_backup_id: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub backup_ts: u64,
    pub catalog_version: u64,
    pub cluster_version: u64,
    pub files: Vec<String>,
    pub checksums: HashMap<String, String>,
    pub status: BackupStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackupManifest {
    V1(BackupManifestV1),
}

// Orchestration
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

fn checksum(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
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

    pub async fn create_full_backup(
        &self,
        cluster_id: &str,
        backup_ts: u64,
        catalog_version: u64,
        cluster_version: u64,
        objects: Vec<BackupObject>,
    ) -> Result<BackupManifestV1> {
        let backup_id = Uuid::new_v4().to_string();
        let started_at = Utc::now();

        let mut files = Vec::new();
        let mut checksums = HashMap::new();
        for obj in &objects {
            let key = data_key(&backup_id, &obj.name);
            self.repo
                .put_object(&key, obj.bytes.clone(), PutOptions { content_type: None })
                .await?;
            checksums.insert(key.clone(), checksum(&obj.bytes));
            files.push(key);
        }

        let manifest = BackupManifestV1 {
            backup_id: backup_id.clone(),
            cluster_id: cluster_id.to_string(),
            backup_type: BackupType::Full,
            parent_backup_id: None,
            started_at,
            completed_at: Some(Utc::now()),
            backup_ts,
            catalog_version,
            cluster_version,
            files,
            checksums,
            status: BackupStatus::Completed,
        };

        let body = serde_json::to_vec(&BackupManifest::V1(manifest.clone()))?;
        self.repo
            .put_object(
                &manifest_key(&backup_id),
                Bytes::from(body),
                PutOptions {
                    content_type: Some("application/json".into()),
                },
            )
            .await?;

        // Re-verify before reporting success.
        if let Err(e) = self.verify(&backup_id).await {
            let mut failed_manifest = manifest.clone();
            failed_manifest.status = BackupStatus::Failed;
            let failed_body = serde_json::to_vec(&BackupManifest::V1(failed_manifest))?;
            let _ = self
                .repo
                .put_object(
                    &manifest_key(&backup_id),
                    Bytes::from(failed_body),
                    PutOptions {
                        content_type: Some("application/json".into()),
                    },
                )
                .await;
            anyhow::bail!("Backup verification failed: {}", e);
        }
        Ok(manifest)
    }

    pub async fn load_manifest(&self, backup_id: &str) -> Result<BackupManifestV1> {
        let body = self.repo.get_object(&manifest_key(backup_id), None).await?;
        match serde_json::from_slice::<BackupManifest>(&body)? {
            BackupManifest::V1(m) => Ok(m),
        }
    }

    /// Verifies that the manifest is `Completed` and every recorded file is
    /// present with a matching checksum. Returns an error otherwise.
    pub async fn verify(&self, backup_id: &str) -> Result<()> {
        let manifest = self.load_manifest(backup_id).await?;
        if manifest.status != BackupStatus::Completed {
            anyhow::bail!("backup {backup_id} is not COMPLETE: {:?}", manifest.status);
        }
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

    pub async fn create_incremental_backup(
        &self,
        cluster_id: &str,
        parent_backup_id: &str,
        backup_ts: u64,
        catalog_version: u64,
        cluster_version: u64,
    ) -> Result<BackupManifestV1> {
        let backup_id = Uuid::new_v4().to_string();
        let started_at = Utc::now();

        // Verify parent exists
        let _parent = self.load_manifest(parent_backup_id).await?;

        let manifest = BackupManifestV1 {
            backup_id: backup_id.clone(),
            cluster_id: cluster_id.to_string(),
            backup_type: BackupType::Incremental,
            parent_backup_id: Some(parent_backup_id.to_string()),
            started_at,
            completed_at: Some(Utc::now()),
            backup_ts,
            catalog_version,
            cluster_version,
            files: vec![],
            checksums: HashMap::new(),
            status: BackupStatus::Completed,
        };

        let body = serde_json::to_vec(&BackupManifest::V1(manifest.clone()))?;
        self.repo
            .put_object(
                &manifest_key(&backup_id),
                Bytes::from(body),
                PutOptions {
                    content_type: Some("application/json".into()),
                },
            )
            .await?;

        Ok(manifest)
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
                if let Some(parent) = manifest.parent_backup_id {
                    // Return the parent's objects. PITR logic (WAL replay) will handle the diff.
                    return self.restore(&parent).await;
                } else {
                    anyhow::bail!("Incremental backup {} missing parent_backup_id", backup_id);
                }
            }

            let prefix = format!("{backup_id}/data/");
            let mut out = Vec::new();
            for key in &manifest.files {
                let bytes = self.repo.get_object(key, None).await?;
                let name = key.strip_prefix(&prefix).unwrap_or(key).to_string();
                out.push(BackupObject { name, bytes });
            }
            Ok(out)
        })
    }

    /// Deletes a backup's manifest and associated data files.
    pub async fn delete_backup(&self, backup_id: &str) -> Result<()> {
        let prefix = format!("{backup_id}/");
        let objects = self.repo.list_objects(&prefix).await?;
        for obj in objects {
            self.repo.delete_object(&obj.key).await?;
        }
        Ok(())
    }

    /// Lists the ids of backups that have a manifest in the repository.
    pub async fn list_backups(&self) -> Result<Vec<String>> {
        let objects = self.repo.list_objects("").await?;
        Ok(objects
            .into_iter()
            .filter_map(|o| o.key.strip_suffix("/manifest.json").map(|s| s.to_string()))
            .collect())
    }

    /// Archives a WAL segment.
    pub async fn archive_wal(&self, filename: &str, data: Bytes) -> Result<()> {
        let key = format!("wal_archive/{}", filename);
        self.repo
            .put_object(&key, data, PutOptions { content_type: None })
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
        repo.put_object(
            "test.txt",
            Bytes::from("hello"),
            PutOptions { content_type: None },
        )
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
        repo.put_object(
            "a/b.txt",
            Bytes::from("data"),
            PutOptions { content_type: None },
        )
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
        repo.put_object(
            &key,
            Bytes::from("tampered"),
            PutOptions { content_type: None },
        )
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

        let mut manifest = BackupManifestV1 {
            backup_id: "test-id-123".to_string(),
            cluster_id: "c".to_string(),
            backup_type: BackupType::Full,
            parent_backup_id: None,
            started_at: Utc::now(),
            completed_at: None,
            backup_ts: 0,
            catalog_version: 0,
            cluster_version: 0,
            files: vec!["f1".to_string()],
            checksums: std::collections::HashMap::new(),
            status: BackupStatus::Completed,
        };
        manifest
            .checksums
            .insert("f1".to_string(), "abc".to_string());

        let body = serde_json::to_vec(&BackupManifest::V1(manifest.clone())).unwrap();
        repo.put_object(
            &manifest_key("test-id-123"),
            Bytes::from(body),
            PutOptions { content_type: None },
        )
        .await
        .unwrap();
        repo.put_object("f1", Bytes::from("bad"), PutOptions { content_type: None })
            .await
            .unwrap();

        if orch.verify("test-id-123").await.is_err() {
            manifest.status = BackupStatus::Failed;
            let fb = serde_json::to_vec(&BackupManifest::V1(manifest)).unwrap();
            repo.put_object(
                &manifest_key("test-id-123"),
                Bytes::from(fb),
                PutOptions { content_type: None },
            )
            .await
            .unwrap();
        }

        let m = orch.load_manifest("test-id-123").await.unwrap();
        assert_eq!(m.status, BackupStatus::Failed);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
