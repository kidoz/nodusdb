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

#[derive(Debug, Clone)]
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
}
