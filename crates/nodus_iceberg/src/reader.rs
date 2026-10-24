use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures_util::StreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::scan::ArrowRecordBatchStream;
use iceberg::spec::{FormatVersion, ManifestContentType, ManifestList};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use serde::Serialize;
use tokio::time::{Instant, timeout_at};

use crate::{CatalogOptions, Error, ReadOptions, operation};

const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;
const MAX_MANIFESTS: usize = 4096;

/// Safe, versioned inspection output. Never includes credentials or properties.
#[derive(Debug, Serialize)]
pub struct ReadSummary {
    /// Version of this CLI inspection contract, not the Iceberg format version.
    pub version: u8,
    /// External namespace components.
    pub namespace: Vec<String>,
    /// External table name.
    pub table: String,
    /// Iceberg table UUID, for identifying replacement tables.
    pub table_uuid: String,
    /// Iceberg format version.
    pub format_version: u8,
    /// Snapshot pinned for this operation, or null for an empty new table.
    pub snapshot_id: Option<i64>,
    /// Schema of the selected snapshot (the current schema for an empty table).
    pub schema: iceberg::spec::SchemaRef,
    /// Selected top-level columns in output order.
    pub columns: Vec<String>,
}

/// Immutable metadata and snapshot selection for one bounded read operation.
pub struct SnapshotReader {
    table: Table,
    options: ReadOptions,
    summary: ReadSummary,
    schema: SchemaRef,
    deadline: Instant,
}

impl SnapshotReader {
    /// Discover catalog configuration and load a table once. The deadline starts
    /// here and is shared by subsequent planning and batch reads. Redirects are
    /// disabled so a catalog cannot redirect the bearer token to another host.
    #[tracing::instrument(skip_all, name = "iceberg.open")]
    pub async fn open(config: CatalogOptions, options: ReadOptions) -> Result<Self, Error> {
        config.validate()?;
        options.validate()?;
        let deadline = Instant::now() + config.timeout;
        let client = iceberg_http::Client::builder()
            .redirect(iceberg_http::redirect::Policy::none())
            .timeout(config.timeout)
            .build()
            .map_err(|_| Error::InvalidOptions("could not construct catalog HTTP client".into()))?;
        let storage = crate::storage::ReadStorageFactory(config.storage);
        let mut properties = config.storage_properties;
        properties.insert(
            REST_CATALOG_PROP_URI.into(),
            config.uri.trim_end_matches('/').into(),
        );
        if let Some(warehouse) = config.warehouse {
            properties.insert(REST_CATALOG_PROP_WAREHOUSE.into(), warehouse);
        }
        if let Some(token) = config.bearer_token {
            properties.insert("token".into(), token);
        }
        properties.insert(
            "header.X-Iceberg-Access-Delegation".into(),
            "vended-credentials".into(),
        );
        let namespace = NamespaceIdent::from_vec(options.namespace.clone())
            .map_err(|e| operation("namespace validation", e))?;
        let identifier = TableIdent::new(namespace, options.table.clone());
        let table = timeout_at(deadline, async {
            let catalog = RestCatalogBuilder::default()
                .with_client(client)
                .with_storage_factory(Arc::new(storage))
                .load("nodus-external", properties)
                .await
                .map_err(|e| operation("catalog configuration", e))?;
            catalog
                .load_table(&identifier)
                .await
                .map_err(|e| operation("load table", e))
        })
        .await
        .map_err(|_| Error::Timeout)??;
        Self::from_table(table, options, deadline)
    }

    fn from_table(table: Table, options: ReadOptions, deadline: Instant) -> Result<Self, Error> {
        let metadata = table.metadata();
        let format_version = match metadata.format_version() {
            FormatVersion::V1 => 1,
            FormatVersion::V2 => 2,
            _ => {
                return Err(Error::Unsupported(
                    "only Iceberg format v1 and v2 are supported",
                ));
            }
        };
        let snapshot = match options.snapshot_id {
            Some(id) => Some(metadata.snapshot_by_id(id).ok_or_else(|| {
                Error::InvalidOptions(format!("snapshot {id} is not retained in this table"))
            })?),
            None => metadata.current_snapshot(),
        };
        let schema = match snapshot {
            Some(snapshot) => snapshot
                .schema(metadata)
                .map_err(|e| operation("snapshot schema", e))?,
            None => metadata.current_schema().clone(),
        };
        let arrow_schema =
            schema_to_arrow_schema(&schema).map_err(|e| operation("Arrow schema", e))?;
        let columns = if options.columns.is_empty() {
            arrow_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        } else {
            options.columns.clone()
        };
        // Validate even a table with no snapshot; upstream otherwise returns an
        // empty stream before checking requested column names.
        let indices = columns
            .iter()
            .map(|name| {
                arrow_schema.index_of(name).map_err(|_| {
                    Error::InvalidOptions(format!(
                        "column {name:?} is not a top-level field in the selected snapshot"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let projected = arrow_schema
            .project(&indices)
            .map_err(|_| Error::InvalidOptions("invalid column projection".into()))?;
        let summary = ReadSummary {
            version: 1,
            namespace: options.namespace.clone(),
            table: options.table.clone(),
            table_uuid: metadata.uuid().to_string(),
            format_version,
            snapshot_id: snapshot.map(|s| s.snapshot_id()),
            schema,
            columns,
        };
        tracing::info!(
            snapshot_id = summary.snapshot_id,
            format_version,
            "Iceberg snapshot selected"
        );
        Ok(Self {
            table,
            options,
            summary,
            schema: Arc::new(projected),
            deadline,
        })
    }

    /// Inspect the pinned snapshot, without opening data files.
    pub fn summary(&self) -> &ReadSummary {
        &self.summary
    }

    /// Arrow output schema, including an empty table's projected schema.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Plan and read the pinned snapshot. The initial subset rejects any delete
    /// manifest, including an empty one, before producing rows. It does not
    /// silently resurrect rows by ignoring equality or positional deletes.
    #[tracing::instrument(skip_all, name = "iceberg.scan")]
    pub async fn scan(self) -> Result<BatchReader, Error> {
        let stream = timeout_at(self.deadline, async {
            if let Some(id) = self.summary.snapshot_id {
                let snapshot =
                    self.table.metadata().snapshot_by_id(id).ok_or_else(|| {
                        Error::InvalidOptions("pinned snapshot is missing".into())
                    })?;
                if snapshot.encryption_key_id().is_some() {
                    return Err(Error::Unsupported("encrypted manifest lists"));
                }
                let input = self
                    .table
                    .file_io()
                    .new_input(snapshot.manifest_list())
                    .map_err(|e| operation("open manifest list", e))?;
                let size = input
                    .metadata()
                    .await
                    .map_err(|e| operation("manifest list size", e))?
                    .size;
                if size > MAX_MANIFEST_BYTES {
                    return Err(Error::Unsupported("manifest lists larger than 16 MiB"));
                }
                // Range reads bound the fetched bytes even if the object changes
                // after the size check. Iceberg metadata objects must be immutable.
                let bytes = input
                    .reader()
                    .await
                    .map_err(|e| operation("manifest list reader", e))?
                    .read(0..size)
                    .await
                    .map_err(|e| operation("read manifest list", e))?;
                let manifests = ManifestList::parse_with_version(
                    &bytes,
                    self.table.metadata().format_version(),
                )
                .map_err(|e| operation("parse manifest list", e))?;
                if manifests.entries().len() > MAX_MANIFESTS {
                    return Err(Error::Unsupported(
                        "snapshots with more than 4096 manifests",
                    ));
                }
                for manifest in manifests.entries() {
                    if manifest.content == ManifestContentType::Deletes {
                        return Err(Error::Unsupported("snapshots with delete manifests"));
                    }
                    if manifest.manifest_length < 0
                        || manifest.manifest_length as u64 > MAX_MANIFEST_BYTES
                    {
                        return Err(Error::Unsupported(
                            "invalid or larger-than-16-MiB manifests",
                        ));
                    }
                }
            }
            let mut scan = self
                .table
                .scan()
                .select(&self.summary.columns)
                .with_batch_size(Some(self.options.batch_size))
                .with_concurrency_limit(1)
                .with_data_file_concurrency_limit(1)
                .with_manifest_entry_concurrency_limit(1);
            if let Some(id) = self.summary.snapshot_id {
                scan = scan.snapshot_id(id);
            }
            scan.build()
                .map_err(|e| operation("build scan", e))?
                .to_arrow()
                .await
                .map_err(|e| operation("plan scan", e))
        })
        .await
        .map_err(|_| Error::Timeout)??;
        Ok(BatchReader {
            stream,
            deadline: self.deadline,
            remaining: self.options.limit,
            columns: self.summary.columns,
            finished: false,
        })
    }
}

/// Incremental batch reader with one operation deadline and optional row limit.
pub struct BatchReader {
    stream: ArrowRecordBatchStream,
    deadline: Instant,
    remaining: Option<usize>,
    columns: Vec<String>,
    finished: bool,
}

impl BatchReader {
    /// Read the next batch. Errors terminate this reader; dropping it cancels
    /// consumption. Memory scales with batches and upstream metadata decoding,
    /// not the total result row count. This is not a hard process-memory sandbox.
    #[tracing::instrument(skip_all, name = "iceberg.next_batch")]
    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>, Error> {
        if self.finished || self.remaining == Some(0) {
            return Ok(None);
        }
        if Instant::now() >= self.deadline {
            self.finished = true;
            return Err(Error::Timeout);
        }
        let result = timeout_at(self.deadline, self.stream.next()).await;
        let batch = match result {
            Err(_) => {
                self.finished = true;
                return Err(Error::Timeout);
            }
            Ok(Some(Err(error))) => {
                self.finished = true;
                return Err(operation("read data batch", error));
            }
            Ok(None) => {
                self.finished = true;
                return Ok(None);
            }
            Ok(Some(Ok(batch))) => batch,
        };
        // Ensure requested order even if the underlying reader chooses schema
        // order when projecting Parquet field IDs.
        // Keep the reader terminal if projection validation below fails.
        self.finished = true;
        let positions: HashMap<_, _> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().clone(), i))
            .collect();
        let indices = self
            .columns
            .iter()
            .map(|name| {
                positions
                    .get(name)
                    .copied()
                    .ok_or(Error::Unsupported("reader returned an inconsistent schema"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut batch = batch
            .project(&indices)
            .map_err(|_| Error::Unsupported("reader returned an inconsistent projection"))?;
        if let Some(remaining) = self.remaining.as_mut() {
            let count = batch.num_rows().min(*remaining);
            batch = batch.slice(0, count);
            *remaining -= count;
        }
        tracing::debug!(
            rows = batch.num_rows(),
            bytes = batch.get_array_memory_size(),
            "Iceberg batch read"
        );
        self.finished = false;
        Ok(Some(batch))
    }
}
