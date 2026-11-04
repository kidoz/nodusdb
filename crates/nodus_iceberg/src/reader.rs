use nodus_common::iceberg::{ScanPredicate, ScanStats};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures_util::{StreamExt, TryStreamExt};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::scan::ArrowRecordBatchStream;
use iceberg::spec::{FormatVersion, ManifestContentType, ManifestList};
use iceberg::table::Table;
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
    /// Current table schema, or the snapshot schema for explicit time travel.
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
    /// here and is shared by subsequent planning and batch reads. HTTP redirects
    /// are disabled and catalog URI overrides must preserve scheme, host and port.
    #[tracing::instrument(skip_all, name = "iceberg.open")]
    pub async fn open(config: CatalogOptions, options: ReadOptions) -> Result<Self, Error> {
        config.validate()?;
        options.validate()?;
        let deadline = Instant::now() + config.timeout;
        let table = timeout_at(deadline, crate::catalog::load_table(config, &options))
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
        let schema = match (options.snapshot_id, snapshot) {
            (Some(_), Some(snapshot)) => snapshot
                .schema(metadata)
                .map_err(|e| operation("snapshot schema", e))?,
            _ => metadata.current_schema().clone(),
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
        self.scan_pruned(vec![]).await
    }

    /// Prune files using registered field identities. No row predicate is passed
    /// to Arrow: callers must apply their exact residual before their row limit.
    #[tracing::instrument(skip_all, name = "iceberg.scan_pruned")]
    pub async fn scan_pruned(self, predicates: Vec<ScanPredicate>) -> Result<BatchReader, Error> {
        let metrics = Arc::new(Metrics::default());
        let observed = metrics.clone();
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
                .with_batch_size(Some(self.options.batch_size))
                .with_concurrency_limit(1)
                .with_data_file_concurrency_limit(1)
                .with_manifest_entry_concurrency_limit(1);
            if let Some(id) = self.summary.snapshot_id {
                scan = scan.snapshot_id(id);
            }
            let snapshot_schema = match self.summary.snapshot_id {
                Some(id) => self
                    .table
                    .metadata()
                    .snapshot_by_id(id)
                    .ok_or(Error::Unsupported("pinned snapshot is missing"))?
                    .schema(self.table.metadata())
                    .map_err(|e| operation("snapshot schema", e))?,
                None => self.summary.schema.clone(),
            };
            if let Some(predicate) =
                crate::pruning::predicate(&predicates, &self.summary.schema, &snapshot_schema)?
            {
                scan = scan.with_filter(predicate);
            }
            let tasks = scan
                .build()
                .map_err(|e| operation("build scan", e))?
                .plan_files()
                .await
                .map_err(|e| operation("plan scan", e))?;
            let schema = self.summary.schema.clone();
            let field_ids = self
                .summary
                .columns
                .iter()
                .map(|name| {
                    schema.field_id_by_name(name).ok_or(Error::Unsupported(
                        "projection is missing from the selected schema",
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            // Iceberg 0.10.1 plans with the snapshot's schema even for current
            // reads. File membership remains snapshot-pinned, while Arrow must
            // resolve names, missing fields and promotions by the read schema's
            // field IDs. Do not rewrite the snapshot or its persisted metadata.
            let tasks = tasks
                .map_ok(move |mut task| {
                    observed.files.fetch_add(1, Ordering::Relaxed);
                    observed
                        .bytes
                        .fetch_add(task.file_size_in_bytes, Ordering::Relaxed);
                    // The upstream task predicate is bound to the snapshot's old
                    // schema. Preserve only file pruning; SQL residuals use the
                    // validated current schema and are evaluated by NodusDB.
                    task.predicate = None;
                    task.schema = schema.clone();
                    task.project_field_ids = field_ids.clone();
                    task
                })
                .boxed();
            self.table
                .reader_builder()
                .with_batch_size(self.options.batch_size)
                .with_data_file_concurrency_limit(1)
                .build()
                .read(tasks)
                .map(|result| result.stream())
                .map_err(|e| operation("read scan", e))
        })
        .await
        .map_err(|_| Error::Timeout)??;
        Ok(BatchReader {
            metrics,
            rows: 0,
            stream,
            deadline: self.deadline,
            remaining: self.options.limit,
            columns: self.summary.columns,
            finished: false,
        })
    }
}

/// Incremental batch reader with one operation deadline and optional row limit.
#[derive(Default)]
struct Metrics {
    files: AtomicU64,
    bytes: AtomicU64,
}

pub struct BatchReader {
    metrics: Arc<Metrics>,
    rows: u64,
    stream: ArrowRecordBatchStream,
    deadline: Instant,
    remaining: Option<usize>,
    columns: Vec<String>,
    finished: bool,
}

impl BatchReader {
    /// Planning counters for consumed tasks, not network bytes or row groups.
    pub fn stats(&self) -> ScanStats {
        ScanStats {
            planned_files: self.metrics.files.load(Ordering::Relaxed),
            planned_file_bytes: self.metrics.bytes.load(Ordering::Relaxed),
            rows_read: self.rows,
        }
    }

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
        self.rows += batch.num_rows() as u64;
        self.finished = false;
        Ok(Some(batch))
    }
}
