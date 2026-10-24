use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use arrow_ipc::writer::StreamWriter;
use clap::{Args, Subcommand, ValueEnum};
use nodus_iceberg::{CatalogOptions, ReadOptions, SnapshotReader, StorageBackend};

#[derive(Subcommand)]
pub(crate) enum IcebergCommand {
    /// Print versioned JSON describing a table and the selected snapshot
    Inspect(Connection),
    /// Read a snapshot into a new Arrow IPC stream file (never overwrites a file)
    Scan {
        #[command(flatten)]
        connection: Connection,
        /// New destination file; published only after a successful scan
        #[arg(long)]
        output: PathBuf,
        /// Maximum output rows; omitted means the complete selected snapshot
        #[arg(long)]
        limit: Option<usize>,
        /// Rows per batch (does not impose a hard memory limit)
        #[arg(long, default_value_t = 1024, value_parser = clap::value_parser!(u32).range(1..=65536))]
        batch_size: u32,
    },
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum Storage {
    S3,
    Local,
}

#[derive(Args)]
pub(crate) struct Connection {
    /// REST base URL before /v1 (Lakekeeper commonly uses /catalog)
    #[arg(long)]
    catalog_uri: String,
    /// Catalog-specific warehouse identifier
    #[arg(long)]
    warehouse: Option<String>,
    /// Namespace component; repeat for a multi-level namespace
    #[arg(long, required = true)]
    namespace: Vec<String>,
    #[arg(long)]
    table: String,
    /// Retained snapshot to read; omission pins the current snapshot at load time
    #[arg(long)]
    snapshot_id: Option<i64>,
    /// Exact top-level column name; repeat to select columns in output order
    #[arg(long = "column")]
    columns: Vec<String>,
    /// Allowed storage backend; local is for trusted development catalogs
    #[arg(long, value_enum, default_value = "s3")]
    storage: Storage,
    /// JSON object of s3.* string properties; keep credential-bearing files private
    #[arg(long)]
    storage_properties: Option<PathBuf>,
    /// Total catalog and data read deadline
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..=86400))]
    timeout_seconds: u64,
}

pub(crate) async fn run(command: &IcebergCommand) -> anyhow::Result<()> {
    tokio::select! {
        result = run_inner(command) => result,
        signal = tokio::signal::ctrl_c() => {
            signal.context("could not listen for cancellation")?;
            anyhow::bail!("Iceberg read cancelled");
        }
    }
}

async fn run_inner(command: &IcebergCommand) -> anyhow::Result<()> {
    let (connection, batch_size, limit) = match command {
        IcebergCommand::Inspect(connection) => (connection, 1024, None),
        IcebergCommand::Scan {
            connection,
            batch_size,
            limit,
            ..
        } => (connection, *batch_size as usize, *limit),
    };
    let mut storage_properties: HashMap<String, String> = HashMap::new();
    if let Some(path) = &connection.storage_properties {
        // Bound configuration input, and don't print invalid JSON containing secrets.
        use tokio::io::AsyncReadExt;
        let file = tokio::fs::File::open(path)
            .await
            .context("open storage properties file")?;
        let mut bytes = Vec::new();
        file.take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .await
            .context("read storage properties file")?;
        anyhow::ensure!(
            bytes.len() <= 1024 * 1024,
            "storage properties exceed 1 MiB"
        );
        storage_properties = serde_json::from_slice(&bytes).map_err(|_| {
            anyhow::anyhow!("storage properties must be a JSON object of string values")
        })?;
    }
    let bearer_token = match std::env::var("NODUS_ICEBERG_TOKEN") {
        Ok(token) => Some(token),
        Err(std::env::VarError::NotPresent) => None,
        Err(_) => anyhow::bail!("NODUS_ICEBERG_TOKEN must contain valid Unicode"),
    };
    let reader = SnapshotReader::open(
        CatalogOptions {
            uri: connection.catalog_uri.clone(),
            warehouse: connection.warehouse.clone(),
            bearer_token,
            storage_properties,
            storage: match connection.storage {
                Storage::S3 => StorageBackend::S3,
                Storage::Local => StorageBackend::Local,
            },
            timeout: Duration::from_secs(connection.timeout_seconds),
        },
        ReadOptions {
            namespace: connection.namespace.clone(),
            table: connection.table.clone(),
            snapshot_id: connection.snapshot_id,
            columns: connection.columns.clone(),
            batch_size,
            limit,
        },
    )
    .await?;
    match command {
        IcebergCommand::Inspect(_) => {
            println!("{}", serde_json::to_string_pretty(reader.summary())?)
        }
        IcebergCommand::Scan { output, .. } => {
            let snapshot_id = reader.summary().snapshot_id;
            let rows = write_scan(reader, output).await?;
            println!(
                "{}",
                serde_json::json!({
                    "version": 1, "snapshot_id": snapshot_id, "rows": rows,
                "output": output.to_string_lossy(), "format": "arrow-ipc-stream"
                })
            );
        }
    }
    Ok(())
}

async fn write_scan(reader: SnapshotReader, output: &Path) -> anyhow::Result<u64> {
    let schema = reader.schema();
    let mut batches = reader.scan().await?;
    let destination = output.to_path_buf();
    let temporary = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        anyhow::ensure!(!destination.try_exists()?, "output file already exists");
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        Ok(tempfile::NamedTempFile::new_in(parent)?)
    })
    .await??;
    let file = temporary.reopen()?;
    let mut writer =
        tokio::task::spawn_blocking(move || StreamWriter::try_new(file, &schema)).await??;
    let mut rows = 0u64;
    while let Some(batch) = batches.next_batch().await? {
        rows += batch.num_rows() as u64;
        writer = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            writer.write(&batch)?;
            Ok(writer)
        })
        .await??;
    }
    let destination = output.to_path_buf();
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        writer.finish()?;
        writer.get_ref().sync_all()?;
        drop(writer);
        // No partial result is published and a concurrent creator is preserved.
        temporary
            .persist_noclobber(destination)
            .map_err(|e| e.error)?;
        Ok(())
    })
    .await??;
    Ok(rows)
}
