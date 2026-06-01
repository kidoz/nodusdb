//! S3-compatible (AWS S3 / MinIO) backup repository backed by `object_store`.

use crate::{BackupRepository, ByteRange, ObjectMetadata, PutOptions};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures_util::StreamExt;
use object_store::{ObjectStore, aws::AmazonS3Builder, path::Path as ObjPath};

/// Connection settings for an S3-compatible object store.
#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    /// Use path-style addressing (`endpoint/bucket/key`), required by MinIO.
    pub path_style: bool,
}

pub struct S3BackupRepository {
    store: object_store::aws::AmazonS3,
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
        Ok(Self { store })
    }
}

#[async_trait]
impl BackupRepository for S3BackupRepository {
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
                size: meta.size as u64,
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
        Ok(self.store.head(&ObjPath::from(key)).await.is_ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BackupOrchestrator;
    use std::sync::Arc;

    /// End-to-end backup round-trip against a real MinIO/S3. Ignored by default;
    /// run with a configured endpoint:
    /// `NODUS_S3_ENDPOINT=http://127.0.0.1:9000 NODUS_S3_BUCKET=nodus \
    ///  NODUS_S3_KEY=nodus NODUS_S3_SECRET=nodus-secret \
    ///  cargo test -p nodus_backup -- --ignored`
    #[tokio::test]
    #[ignore = "requires a running S3/MinIO endpoint"]
    async fn s3_backup_round_trip() {
        let cfg = S3Config {
            endpoint: std::env::var("NODUS_S3_ENDPOINT").unwrap(),
            bucket: std::env::var("NODUS_S3_BUCKET").unwrap(),
            region: std::env::var("NODUS_S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
            access_key: std::env::var("NODUS_S3_KEY").unwrap(),
            secret_key: std::env::var("NODUS_S3_SECRET").unwrap(),
            path_style: true,
        };
        let repo: Arc<dyn BackupRepository> = Arc::new(S3BackupRepository::new(cfg).unwrap());
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
}
