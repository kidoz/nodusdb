//! Private, bounded SQL bridge. No upstream diagnostic or credential is printed.
use arrow_array::{Array, BinaryArray, LargeBinaryArray};
use arrow_schema::DataType;
use nodus_common::iceberg::{self as wire, Field, Request, Response};
use nodus_iceberg::{CatalogOptions, ReadOptions, SnapshotReader, StorageBackend};
use std::time::Duration;

pub(super) async fn run() -> anyhow::Result<()> {
    let result = inner().await;
    if let Err(error) = result {
        // Only our fixed classifications cross the process boundary. Upstream
        // errors may contain HTTP bodies, object URLs or credential properties.
        let safe = match error.downcast_ref::<WorkerError>() {
            Some(error) => error.0,
            None if matches!(
                error.downcast_ref::<nodus_iceberg::Error>(),
                Some(nodus_iceberg::Error::Timeout)
            ) =>
            {
                "canceling statement due to statement timeout"
            }
            None if matches!(
                error.downcast_ref::<nodus_iceberg::Error>(),
                Some(nodus_iceberg::Error::Unsupported(_))
            ) =>
            {
                "Iceberg snapshot feature is not supported"
            }
            None => "Iceberg worker read failed",
        };
        wire::write_frame(&mut std::io::stdout().lock(), &Response::Error(safe.into()))?;
    }
    Ok(())
}
#[derive(Debug)]
struct WorkerError(&'static str);
impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for WorkerError {}
fn fail(message: &'static str) -> anyhow::Error {
    WorkerError(message).into()
}

async fn inner() -> anyhow::Result<()> {
    let request: Request = wire::read_frame(&mut std::io::stdin().lock())?;
    if request.version != wire::VERSION {
        return Err(fail("unsupported Iceberg worker version"));
    }
    #[cfg(target_os = "linux")]
    rustix::process::setrlimit(
        rustix::process::Resource::As,
        rustix::process::Rlimit {
            current: Some(request.address_space_bytes),
            maximum: Some(request.address_space_bytes),
        },
    )?;
    let reader = SnapshotReader::open(
        CatalogOptions {
            uri: request.profile.uri,
            warehouse: request.profile.warehouse,
            bearer_token: request.profile.bearer_token,
            storage_properties: request.profile.storage_properties,
            storage: StorageBackend::S3,
            timeout: Duration::from_millis(request.timeout_ms),
        },
        ReadOptions {
            namespace: request.namespace,
            table: request.table,
            snapshot_id: None,
            columns: request.columns.clone(),
            batch_size: 64,
            limit: Some(request.limit),
        },
    )
    .await?;
    let fields = reader
        .schema()
        .fields()
        .iter()
        .map(|field| {
            Ok(Field {
                id: reader
                    .summary()
                    .schema
                    .field_id_by_name(field.name())
                    .ok_or_else(|| fail("Iceberg field identity is missing"))?,
                source_type: format!("{:?}", field.data_type()),
                name: field.name().clone(),
                sql_type: sql_type(field.data_type())?.into(),
                nullable: field.is_nullable(),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    if fields.is_empty() || fields.len() > 256 {
        return Err(fail("Iceberg supports 1 to 256 primitive columns"));
    }
    if let Some(expected) = &request.expected
        && (expected.version != wire::VERSION
            || expected.table_uuid != reader.summary().table_uuid
            || fields.iter().any(|field| !expected.fields.contains(field)))
    {
        return Err(fail(
            "Iceberg table identity or schema changed; register the view again",
        ));
    }
    let mut output = std::io::stdout().lock();
    wire::write_frame(
        &mut output,
        &Response::Schema {
            version: wire::VERSION,
            table_uuid: reader.summary().table_uuid.clone(),
            fields,
            snapshot_id: reader.summary().snapshot_id,
        },
    )?;
    if request.expected.is_none() {
        wire::write_frame(&mut output, &Response::Done { rows: 0 })?;
        return Ok(());
    }
    let mut scan = reader.scan().await?;
    let mut count = 0;
    while let Some(batch) = scan.next_batch().await? {
        for index in 0..batch.num_rows() {
            let mut size = 0;
            let mut row = Vec::with_capacity(batch.num_columns());
            for column in batch.columns() {
                let value = if column.is_null(index) {
                    None
                } else {
                    let text = if let Some(binary) = column.as_any().downcast_ref::<BinaryArray>() {
                        hex(binary.value(index))?
                    } else if let Some(binary) = column.as_any().downcast_ref::<LargeBinaryArray>()
                    {
                        hex(binary.value(index))?
                    } else {
                        arrow_cast::display::array_value_to_string(column.as_ref(), index)?
                    };
                    size += text.len() + 64;
                    if size > 16 * 1024 {
                        return Err(fail("Iceberg row exceeds 16 KiB"));
                    }
                    Some(text)
                };
                row.push(value);
            }
            wire::write_frame(&mut output, &Response::Row(row))?;
            count += 1;
        }
    }
    wire::write_frame(&mut output, &Response::Done { rows: count })?;
    Ok(())
}
fn hex(bytes: &[u8]) -> anyhow::Result<String> {
    if bytes.len() > 8 * 1024 - 2 {
        return Err(fail("Iceberg row exceeds 16 KiB"));
    }
    use std::fmt::Write;
    let mut text = String::with_capacity(2 + bytes.len() * 2);
    text.push_str("\\x");
    for byte in bytes {
        write!(text, "{byte:02x}")?;
    }
    Ok(text)
}
fn sql_type(data_type: &DataType) -> anyhow::Result<&'static str> {
    Ok(match data_type {
        DataType::Boolean => "BOOLEAN",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::Float32 => "REAL",
        DataType::Float64 => "DOUBLE PRECISION",
        DataType::Utf8 | DataType::LargeUtf8 => "TEXT",
        DataType::Binary | DataType::LargeBinary => "BYTEA",
        DataType::Date32 => "DATE",
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => "TIMESTAMP",
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some(_)) => "TIMESTAMPTZ",
        // Field.source_type retains precision/scale for drift validation.
        DataType::Decimal128(_, _) => "NUMERIC",
        _ => return Err(fail("Iceberg SQL type is not supported")),
    })
}
