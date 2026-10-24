use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, ManifestListWriter, ManifestWriterBuilder,
    NestedField, PartitionSpec, PrimitiveType, Schema, Struct, Type,
};
use nodus_iceberg::{CatalogOptions, ReadOptions, StorageBackend};
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};

#[derive(Clone)]
pub struct ServerState {
    pub response: Arc<Mutex<Value>>,
    pub requests: Arc<Mutex<Vec<(String, String)>>>,
    pub status: Arc<Mutex<StatusCode>>,
    pub delay: Arc<Mutex<Duration>>,
    pub files: std::path::PathBuf,
}

pub struct Fixture {
    pub directory: tempfile::TempDir,
    pub uri: String,
    pub state: ServerState,
    task: tokio::task::JoinHandle<()>,
    s3: bool,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Fixture {
    pub async fn new() -> Self {
        Self::build(false, false).await
    }

    pub async fn new_s3() -> Self {
        Self::build(true, false).await
    }

    pub async fn new_v1() -> Self {
        Self::build(false, true).await
    }

    async fn build(s3: bool, v1: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_str().unwrap();
        let location = if s3 {
            "s3://test-bucket/warehouse"
        } else {
            root
        };
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields([
                    Arc::new(NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "name",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let io = FileIO::new_with_fs();
        let mut snapshots = vec![];
        // Two distinct snapshots: old rows must remain readable after the
        // current snapshot moves. Files preserve Iceberg field IDs in Parquet.
        for id in [10i64, 20] {
            let path = format!("{root}/data-{id}.parquet");
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![id, id + 1, id + 2])),
                    Arc::new(StringArray::from(vec![Some("alpha"), None, Some("")])),
                ],
            )
            .unwrap();
            let mut parquet = ArrowWriter::try_new(
                std::fs::File::create(&path).unwrap(),
                arrow_schema.clone(),
                None,
            )
            .unwrap();
            parquet.write(&batch).unwrap();
            parquet.close().unwrap();
            let builder = ManifestWriterBuilder::new(
                io.new_output(format!("{root}/manifest-{id}.avro")).unwrap(),
                Some(id),
                schema.clone(),
                PartitionSpec::unpartition_spec(),
            );
            let mut manifest = if v1 {
                builder.build_v1()
            } else {
                builder.build_v2_data()
            };
            let file = DataFileBuilder::default()
                .partition_spec_id(0)
                .content(DataContentType::Data)
                .file_path(format!("{location}/data-{id}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(std::fs::metadata(path).unwrap().len())
                .record_count(3)
                .partition(Struct::empty())
                .build()
                .unwrap();
            manifest.add_file(file, id / 10).unwrap();
            // A deleted data-file entry is not a row-level delete manifest. It
            // must be skipped, even when the referenced file no longer exists.
            let deleted = DataFileBuilder::default()
                .partition_spec_id(0)
                .content(DataContentType::Data)
                .file_path(format!("{location}/removed.parquet"))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(1)
                .partition(Struct::empty())
                .build()
                .unwrap();
            manifest.add_delete_file(deleted, 0, Some(0)).unwrap();
            let mut manifest = manifest.write_manifest_file().await.unwrap();
            manifest.manifest_path = format!("{location}/manifest-{id}.avro");
            let list_path = format!("{root}/list-{id}.avro");
            let writer = io.new_output(&list_path).unwrap().writer().await.unwrap();
            let mut list = if v1 {
                ManifestListWriter::v1(writer, id, None)
            } else {
                ManifestListWriter::v2(writer, id, None, id / 10)
            };
            list.add_manifests(std::iter::once(manifest)).unwrap();
            list.close().await.unwrap();
            snapshots.push(json!({
                "snapshot-id": id, "sequence-number": id / 10, "timestamp-ms": id * 1000,
                "manifest-list": format!("{location}/list-{id}.avro"), "summary": {"operation": "append"}, "schema-id": 0
            }));
        }
        let mut response = json!({
            "metadata-location": format!("{location}/metadata.json"),
            "metadata": {
                "format-version": 2, "table-uuid": "10203040-5060-4070-8090-102030405060",
                "location": location, "last-sequence-number": 2, "last-updated-ms": 20000,
                "last-column-id": 2, "current-schema-id": 0, "schemas": [schema],
                "default-spec-id": 0, "partition-specs": [{"spec-id": 0, "fields": []}],
                "last-partition-id": 999, "default-sort-order-id": 0,
                "sort-orders": [{"order-id": 0, "fields": []}],
                "current-snapshot-id": 20, "snapshots": snapshots,
                "snapshot-log": [{"snapshot-id": 10, "timestamp-ms": 10000}, {"snapshot-id": 20, "timestamp-ms": 20000}],
                "metadata-log": [], "refs": {"main": {"snapshot-id": 20, "type": "branch"}}
            },
            "config": {}
        });
        if v1 {
            response["metadata"]["format-version"] = json!(1);
            response["metadata"]["schema"] = response["metadata"]["schemas"][0].clone();
            response["metadata"]["partition-spec"] = json!([]);
        }
        let state = ServerState {
            response: Arc::new(Mutex::new(response)),
            requests: Arc::new(Mutex::new(vec![])),
            status: Arc::new(Mutex::new(StatusCode::OK)),
            delay: Arc::new(Mutex::new(Duration::ZERO)),
            files: directory.path().to_owned(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri = format!("http://{}/catalog", listener.local_addr().unwrap());
        if s3 {
            state.response.lock().unwrap()["config"] = json!({
                "s3.endpoint": format!("http://{}", listener.local_addr().unwrap()),
                "s3.region": "us-east-1", "s3.access-key-id": "catalog-access",
                "s3.secret-access-key": "catalog-secret", "s3.session-token": "catalog-session",
                "s3.path-style-access": "true", "s3.allow-http": "true"
            });
        }
        let router = Router::new().fallback(handler).with_state(state.clone());
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Self {
            directory,
            uri,
            state,
            task,
            s3,
        }
    }

    pub fn config(&self) -> CatalogOptions {
        CatalogOptions {
            uri: self.uri.clone(),
            warehouse: Some("test-warehouse".into()),
            bearer_token: Some("catalog-token".into()),
            storage_properties: HashMap::new(),
            storage: if self.s3 {
                StorageBackend::S3
            } else {
                StorageBackend::Local
            },
            timeout: Duration::from_secs(10),
        }
    }

    pub fn options(&self) -> ReadOptions {
        ReadOptions {
            namespace: vec!["analytics".into()],
            table: "events".into(),
            snapshot_id: None,
            columns: vec![],
            batch_size: 2,
            limit: None,
        }
    }

    pub fn make_empty(&self) {
        let mut response = self.state.response.lock().unwrap();
        let metadata = &mut response["metadata"];
        metadata
            .as_object_mut()
            .unwrap()
            .remove("current-snapshot-id");
        metadata["snapshots"] = json!([]);
        metadata["snapshot-log"] = json!([]);
        metadata["refs"] = json!({});
    }

    pub async fn add_delete_manifest(&self) {
        use iceberg::spec::{ManifestContentType, ManifestList};
        let path = self.directory.path().join("list-20.avro");
        let bytes = std::fs::read(&path).unwrap();
        let list =
            ManifestList::parse_with_version(&bytes, iceberg::spec::FormatVersion::V2).unwrap();
        let mut manifest = list.entries()[0].clone();
        manifest.content = ManifestContentType::Deletes;
        let io = FileIO::new_with_fs();
        let mut writer = ManifestListWriter::v2(
            io.new_output(path.to_str().unwrap())
                .unwrap()
                .writer()
                .await
                .unwrap(),
            20,
            None,
            2,
        );
        writer.add_manifests(std::iter::once(manifest)).unwrap();
        writer.close().await.unwrap();
    }
}

async fn handler(State(state): State<ServerState>, request: Request) -> Response {
    let delay = *state.delay.lock().unwrap();
    tokio::time::sleep(delay).await;
    state
        .requests
        .lock()
        .unwrap()
        .push((request.method().to_string(), request.uri().to_string()));
    if let Some(key) = request.uri().path().strip_prefix("/test-bucket/warehouse/") {
        // A minimal S3 HTTP fixture verifies range reads and use of credentials
        // returned by load-table. It does not validate AWS signature cryptography.
        assert!(
            request.headers()["authorization"]
                .to_str()
                .unwrap()
                .contains("Credential=catalog-access/")
        );
        assert_eq!(request.headers()["x-amz-security-token"], "catalog-session");
        if key.contains('/') || key.contains("..") {
            return StatusCode::NOT_FOUND.into_response();
        }
        let bytes = tokio::fs::read(state.files.join(key)).await.unwrap();
        let total = bytes.len();
        let mut response = Response::builder()
            .header("etag", "\"test-etag\"")
            .header("last-modified", "Thu, 01 Jan 1970 00:00:00 GMT")
            .header("accept-ranges", "bytes");
        if request.method() == "HEAD" {
            return response
                .header("content-length", total)
                .body(axum::body::Body::empty())
                .unwrap();
        }
        if let Some(range) = request.headers().get("range") {
            let (start, end) = range
                .to_str()
                .unwrap()
                .strip_prefix("bytes=")
                .unwrap()
                .split_once('-')
                .unwrap();
            let start: usize = start.parse().unwrap();
            let end: usize = end.parse().unwrap();
            response = response
                .status(StatusCode::PARTIAL_CONTENT)
                .header("content-range", format!("bytes {start}-{end}/{total}"));
            return response
                .header("content-length", end - start + 1)
                .body(axum::body::Body::from(bytes[start..=end].to_vec()))
                .unwrap();
        }
        return response
            .header("content-length", total)
            .body(axum::body::Body::from(bytes))
            .unwrap();
    }
    if request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        != Some("Bearer catalog-token")
    {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error":{"message":"secret must not leak", "type":"Unauthorized", "code":401}}))).into_response();
    }
    if request.uri().path() == "/catalog/v1/config" {
        assert_eq!(request.uri().query(), Some("warehouse=test-warehouse"));
        return Json(json!({"defaults": {}, "overrides": {"prefix": "warehouse-prefix"}}))
            .into_response();
    }
    if request.uri().path() == "/catalog/v1/warehouse-prefix/namespaces/analytics/tables/events" {
        let status = *state.status.lock().unwrap();
        if status != StatusCode::OK {
            return (status, Json(json!({"error":{"message":"catalog-token private data", "type":"TestFailure", "code":status.as_u16()}}))).into_response();
        }
        assert_eq!(
            request
                .headers()
                .get("x-iceberg-access-delegation")
                .unwrap(),
            "vended-credentials"
        );
        return Json(state.response.lock().unwrap().clone()).into_response();
    }
    StatusCode::NOT_FOUND.into_response()
}
