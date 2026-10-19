//! S3-compatible (AWS S3 / MinIO) backup repository backed by `object_store`.

use crate::{BackupRepository, ByteRange, ObjectMetadata, PutOptions, RepositoryCapabilities};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures_util::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, path::Path as ObjPath};
use std::sync::Arc;

/// Connection settings for an S3-compatible object store.
#[derive(Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    /// Use path-style addressing (`endpoint/bucket/key`), required by MinIO.
    pub path_style: bool,
}

// Hand-written so a `{:?}` (in a log line, panic, or error chain) can never leak
// the AWS access/secret keys held here.
impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("access_key", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .field("path_style", &self.path_style)
            .finish()
    }
}

pub struct S3BackupRepository {
    store: Arc<dyn ObjectStore>,
}

impl S3BackupRepository {
    pub fn new(cfg: S3Config) -> Result<Self> {
        let store = AmazonS3Builder::new()
            .with_endpoint(cfg.endpoint)
            .with_bucket_name(cfg.bucket)
            .with_region(cfg.region)
            .with_access_key_id(cfg.access_key)
            .with_secret_access_key(cfg.secret_key)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(!cfg.path_style)
            .build()?;
        Ok(Self {
            store: Arc::new(store),
        })
    }

    /// Builds a repository over any `object_store` backend. The S3 wire protocol
    /// still needs a live endpoint, but this lets the repository's put/get/list/
    /// delete logic run in CI against, e.g., `object_store::memory::InMemory`.
    pub fn with_store(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Builds a repository for `bucket` (optionally rooted under `prefix`)
    /// from the standard AWS-compatible environment variables consumed by
    /// [`AmazonS3Builder::from_env`]: `AWS_ACCESS_KEY_ID`,
    /// `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` (default `us-east-1`),
    /// `AWS_ENDPOINT_URL`/`AWS_ENDPOINT`, `AWS_ALLOW_HTTP` for plain-HTTP
    /// endpoints such as MinIO, plus the usual session-token, profile, and
    /// addressing-style variables. Credentials never enter the server
    /// configuration file. An empty `prefix` addresses the bucket root.
    pub fn from_env(bucket: &str, prefix: &str) -> Result<Self> {
        let store: Arc<dyn ObjectStore> = Arc::new(
            AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()?,
        );
        let store = if prefix.is_empty() {
            store
        } else {
            Arc::new(object_store::prefix::PrefixStore::new(store, prefix))
        };
        Ok(Self { store })
    }
}

#[async_trait]
impl BackupRepository for S3BackupRepository {
    fn capabilities(&self) -> RepositoryCapabilities {
        RepositoryCapabilities {
            immutable_objects: false,
            conditional_put: false,
            range_reads: true,
            multipart_upload: false,
            server_side_encryption: false,
        }
    }

    async fn put_object(
        &self,
        key: &str,
        body: Bytes,
        _options: PutOptions,
    ) -> Result<ObjectMetadata> {
        let size = body.len() as u64;
        self.store.put(&ObjPath::from(key), body.into()).await?;
        Ok(ObjectMetadata {
            key: key.to_string(),
            size,
            last_modified: Utc::now(),
        })
    }

    async fn get_object(&self, key: &str, range: Option<ByteRange>) -> Result<Bytes> {
        let result = self.store.get(&ObjPath::from(key)).await?;
        let bytes = result.bytes().await?;
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
        let prefix_path = if prefix.is_empty() {
            None
        } else {
            Some(ObjPath::from(prefix))
        };
        let mut stream = self.store.list(prefix_path.as_ref());
        let mut out = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            out.push(ObjectMetadata {
                key: meta.location.to_string(),
                size: meta.size,
                last_modified: meta.last_modified,
            });
        }
        Ok(out)
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        self.store.delete(&ObjPath::from(key)).await?;
        Ok(())
    }

    async fn object_exists(&self, key: &str) -> Result<bool> {
        // Only genuine absence counts as missing. Network errors, 403s, and
        // throttling propagate instead: the fail-closed protection paths
        // treat an error as "unknown" (protective), whereas mapping every
        // error to `false` would flip manifests to unreadable — or, worse,
        // report a needed object as gone — on a transient S3 outage.
        match self.store.head(&ObjPath::from(key)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BackupOrchestrator;
    use nodus_storage_api::KvEngine;
    use std::sync::Arc;

    /// CI-runnable repository round-trip: drives the same `S3BackupRepository`
    /// put/get/list/delete code the S3 path uses, but over an in-memory object
    /// store so no network or MinIO is required. The environment-gated live test
    /// below separately exercises the real S3 wire protocol.
    #[tokio::test]
    async fn s3_repository_round_trip_in_memory() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let repo: Arc<dyn BackupRepository> = Arc::new(S3BackupRepository::with_store(store));

        // Direct repository operations.
        repo.put_object("a/b.txt", Bytes::from("hello"), PutOptions::default())
            .await
            .unwrap();
        assert!(repo.object_exists("a/b.txt").await.unwrap());
        assert_eq!(repo.get_object("a/b.txt", None).await.unwrap(), "hello");
        // Range read.
        assert_eq!(
            repo.get_object("a/b.txt", Some(ByteRange { start: 1, end: 3 }))
                .await
                .unwrap(),
            Bytes::from("ell")
        );
        assert_eq!(repo.list_objects("a/").await.unwrap().len(), 1);
        repo.delete_object("a/b.txt").await.unwrap();
        assert!(!repo.object_exists("a/b.txt").await.unwrap());

        // The full orchestration (backup → verify → restore) over the same store.
        let orch = BackupOrchestrator::new(repo);
        let manifest = orch
            .create_full_backup(
                "cluster-1",
                1,
                1,
                1,
                vec![crate::BackupObject {
                    name: "catalog".into(),
                    bytes: Bytes::from("data"),
                }],
            )
            .await
            .unwrap();
        orch.verify(&manifest.backup_id).await.unwrap();
        assert_eq!(orch.restore(&manifest.backup_id).await.unwrap().len(), 1);
    }

    /// Live S3/MinIO coverage is enabled explicitly by NODUS_S3_ENDPOINT. CI
    /// supplies an isolated bucket; a unique prefix also isolates local runs that
    /// share a development bucket.
    #[tokio::test]
    async fn s3_live_backup_verify_restore_delete_and_failures() {
        let Ok(endpoint) = std::env::var("NODUS_S3_ENDPOINT") else {
            eprintln!("skipping live S3 test: NODUS_S3_ENDPOINT is not set");
            return;
        };
        let cfg = S3Config {
            endpoint,
            bucket: std::env::var("NODUS_S3_BUCKET")
                .expect("NODUS_S3_BUCKET is required with NODUS_S3_ENDPOINT"),
            region: std::env::var("NODUS_S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
            access_key: std::env::var("NODUS_S3_KEY")
                .expect("NODUS_S3_KEY is required with NODUS_S3_ENDPOINT"),
            secret_key: std::env::var("NODUS_S3_SECRET")
                .expect("NODUS_S3_SECRET is required with NODUS_S3_ENDPOINT"),
            path_style: true,
        };
        let base = S3BackupRepository::new(cfg.clone()).unwrap();
        let prefix = format!("live-test-{}", uuid::Uuid::new_v4());
        let store = Arc::new(object_store::prefix::PrefixStore::new(base.store, prefix));
        let repo: Arc<dyn BackupRepository> = Arc::new(S3BackupRepository::with_store(store));
        let orch = BackupOrchestrator::new(repo.clone());

        let dump = serde_json::to_vec(&serde_json::json!([
            {"key": b"row".to_vec(), "value": b"value".to_vec(), "deleted": false, "version": 10},
            {"key": b"deleted-row".to_vec(), "value": null, "deleted": true, "version": 11}
        ]))
        .unwrap();
        let manifest = orch
            .create_full_backup(
                "cluster-1",
                11,
                1,
                1,
                vec![crate::BackupObject {
                    name: "kv_data.json".into(),
                    bytes: Bytes::from(dump),
                }],
            )
            .await
            .unwrap();
        orch.verify(&manifest.backup_id).await.unwrap();

        let restored = orch.restore(&manifest.backup_id).await.unwrap();
        let kv = nodus_storage_mem::MemKvEngine::new();
        let report = BackupOrchestrator::restore_backup_objects_to_kv(&restored, &kv).unwrap();
        assert_eq!(report.base_kv_versions_restored, 2);
        assert_eq!(
            kv.get(b"row", u64::MAX).unwrap().as_deref(),
            Some(b"value".as_slice())
        );
        assert!(kv.get(b"deleted-row", u64::MAX).unwrap().is_none());

        let data_key = manifest.files[0].clone();
        let original = repo.get_object(&data_key, None).await.unwrap();
        repo.put_object(&data_key, Bytes::from("corrupt"), PutOptions::default())
            .await
            .unwrap();
        assert!(orch.verify(&manifest.backup_id).await.is_err());
        assert!(orch.restore(&manifest.backup_id).await.is_err());
        repo.put_object(&data_key, original, PutOptions::default())
            .await
            .unwrap();
        orch.verify(&manifest.backup_id).await.unwrap();

        let unavailable = S3BackupRepository::new(S3Config {
            endpoint: "http://127.0.0.1:9".into(),
            ..cfg
        })
        .unwrap();
        let unavailable_result = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            unavailable.object_exists("probe"),
        )
        .await
        .expect("unavailable S3 endpoint did not fail within 30 seconds");
        assert!(
            unavailable_result.is_err(),
            "transport failures must propagate instead of looking like object absence"
        );

        orch.delete_backup(&manifest.backup_id).await.unwrap();
        assert!(orch.list_backups().await.unwrap().is_empty());
        assert!(!repo.object_exists(&data_key).await.unwrap());
        assert!(orch.verify(&manifest.backup_id).await.is_err());
    }
}
