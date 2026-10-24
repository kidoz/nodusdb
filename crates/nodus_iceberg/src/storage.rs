//! Read-only bridge to the same object_store implementation used by backups.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use object_store::{
    ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, local::LocalFileSystem, path::Path,
};
use serde::{Deserialize, Serialize};

use crate::StorageBackend;

const MAX_READ_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ReadStorageFactory(pub StorageBackend);

#[typetag::serde(name = "NodusIcebergReadStorageFactory")]
impl StorageFactory for ReadStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let properties: HashMap<_, _> = config
            .props()
            .iter()
            .filter(|(key, _)| key.starts_with("s3."))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        const SUPPORTED: &[&str] = &[
            "s3.endpoint",
            "s3.region",
            "s3.access-key-id",
            "s3.secret-access-key",
            "s3.session-token",
            "s3.path-style-access",
            "s3.allow-http",
        ];
        if properties
            .keys()
            .any(|key| !SUPPORTED.contains(&key.as_str()))
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "unsupported S3 property; supported: endpoint, region, access-key-id, secret-access-key, session-token, path-style-access, allow-http",
            ));
        }
        Ok(Arc::new(ReadStorage {
            backend: self.0,
            properties,
        }))
    }
}

// typetag is required by Iceberg's storage trait. No NodusDB durable/network
// record serializes this value; credentials must stay local to the read client.
#[derive(Clone, Serialize, Deserialize)]
struct ReadStorage {
    backend: StorageBackend,
    properties: HashMap<String, String>,
}

impl fmt::Debug for ReadStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadStorage")
            .field("backend", &self.backend)
            .finish_non_exhaustive()
    }
}

fn failure(_: object_store::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "object storage read failed")
}

fn unsupported(message: &'static str) -> Error {
    Error::new(ErrorKind::FeatureUnsupported, message)
}

impl ReadStorage {
    fn resolve(&self, location: &str) -> Result<(Arc<dyn ObjectStore>, Path)> {
        match self.backend {
            StorageBackend::Local => {
                let path = if location.starts_with("file:") {
                    iceberg_http::Url::parse(location)
                        .ok()
                        .and_then(|url| url.to_file_path().ok())
                        .ok_or_else(|| unsupported("invalid local file URI"))?
                } else {
                    std::path::PathBuf::from(location)
                };
                if !path.is_absolute() {
                    return Err(unsupported(
                        "local mode requires an absolute filesystem path",
                    ));
                }
                let path = Path::from_absolute_path(path)
                    .map_err(|_| unsupported("invalid local path"))?;
                Ok((Arc::new(LocalFileSystem::new()), path))
            }
            StorageBackend::S3 => {
                let url = iceberg_http::Url::parse(location)
                    .map_err(|_| unsupported("invalid S3 URI"))?;
                if !matches!(url.scheme(), "s3" | "s3a" | "s3n")
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.query().is_some()
                    || url.fragment().is_some()
                    || url.port().is_some()
                {
                    return Err(unsupported("S3 mode requires an s3://bucket/key URI"));
                }
                let bucket = url
                    .host_str()
                    .ok_or_else(|| unsupported("missing S3 bucket"))?;
                let path =
                    Path::from_url_path(url.path()).map_err(|_| unsupported("invalid S3 key"))?;
                let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
                for (key, value) in &self.properties {
                    builder = match key.as_str() {
                        "s3.endpoint" => builder.with_endpoint(value),
                        "s3.region" => builder.with_region(value),
                        "s3.access-key-id" => builder.with_access_key_id(value),
                        "s3.secret-access-key" => builder.with_secret_access_key(value),
                        "s3.session-token" => builder.with_token(value),
                        "s3.path-style-access" => {
                            builder.with_virtual_hosted_style_request(!parse_bool(value)?)
                        }
                        "s3.allow-http" => builder.with_allow_http(parse_bool(value)?),
                        _ => return Err(unsupported("unsupported S3 property")),
                    };
                }
                Ok((Arc::new(builder.build().map_err(failure)?), path))
            }
        }
    }
}

fn parse_bool(value: &str) -> Result<bool> {
    value
        .parse()
        .map_err(|_| unsupported("S3 boolean properties require true or false"))
}

struct ObjectReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

#[async_trait]
impl FileRead for ObjectReader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        if range.end < range.start || range.end - range.start > MAX_READ_BYTES {
            return Err(unsupported("single storage reads larger than 64 MiB"));
        }
        self.store
            .get_range(&self.path, range)
            .await
            .map_err(failure)
    }
}

#[async_trait]
#[typetag::serde(name = "NodusIcebergReadStorage")]
impl Storage for ReadStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (store, path) = self.resolve(path)?;
        match store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(failure(error)),
        }
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (store, path) = self.resolve(path)?;
        Ok(FileMetadata {
            size: store.head(&path).await.map_err(failure)?.size,
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let size = self.metadata(path).await?.size;
        self.reader(path).await?.read(0..size).await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (store, path) = self.resolve(path)?;
        Ok(Box::new(ObjectReader { store, path }))
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_owned()))
    }

    fn new_output(&self, _: &str) -> Result<OutputFile> {
        Err(unsupported("read-only Iceberg storage"))
    }

    async fn write(&self, _: &str, _: Bytes) -> Result<()> {
        Err(unsupported("read-only Iceberg storage"))
    }
    async fn writer(&self, _: &str) -> Result<Box<dyn FileWrite>> {
        Err(unsupported("read-only Iceberg storage"))
    }
    async fn delete(&self, _: &str) -> Result<()> {
        Err(unsupported("read-only Iceberg storage"))
    }
    async fn delete_prefix(&self, _: &str) -> Result<()> {
        Err(unsupported("read-only Iceberg storage"))
    }
    async fn delete_stream(&self, _: BoxStream<'static, String>) -> Result<()> {
        Err(unsupported("read-only Iceberg storage"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_only_storage_refuses_every_mutation() {
        let storage = ReadStorage {
            backend: StorageBackend::Local,
            properties: HashMap::new(),
        };
        assert!(storage.new_output("/tmp/not-created").is_err());
        assert!(
            storage
                .write("/tmp/not-created", Bytes::new())
                .await
                .is_err()
        );
        assert!(storage.writer("/tmp/not-created").await.is_err());
        assert!(storage.delete("/tmp/not-deleted").await.is_err());
        assert!(storage.delete_prefix("/tmp").await.is_err());
        assert!(
            storage
                .delete_stream(Box::pin(futures_util::stream::empty()))
                .await
                .is_err()
        );
    }

    #[test]
    fn s3_backend_cannot_open_local_files_and_debug_redacts_credentials() {
        let storage = ReadStorage {
            backend: StorageBackend::S3,
            properties: HashMap::from([("s3.secret-access-key".into(), "secret-marker".into())]),
        };
        assert!(storage.resolve("file:///etc/passwd").is_err());
        assert!(!format!("{storage:?}").contains("secret-marker"));
    }
}
