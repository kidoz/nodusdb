//! COPY output through the bounded executor streaming bridge. Binary COPY
//! reuses binary DataRow fields; text and CSV escape text fields per COPY rules.

use crate::{
    CurrentQueryGuard,
    client_meta::*,
    streaming::{join_producer, start_row_stream},
    wire_format::{sqlstate_for_execution_error, user_error},
};
use bytes::{Buf, BufMut, BytesMut};
use futures_util::{Sink, SinkExt};
use nodus_executor::{CopyOutputFormat, ExecutionContext, Executor};
use nodus_security::SessionRegistry;
use pgwire::{
    api::{ClientInfo, portal::Format, results::FieldFormat},
    error::{PgWireError, PgWireResult},
    messages::{
        PgWireBackendMessage,
        copy::{CopyData, CopyDone, CopyOutResponse},
        data::DataRow,
        response::CommandComplete,
    },
};
use std::{fmt::Debug, sync::Arc};

fn copy_row(mut row: DataRow, format: CopyOutputFormat) -> PgWireResult<BytesMut> {
    let mut output = BytesMut::new();
    if format == CopyOutputFormat::Binary {
        output.put_i16(row.field_count);
        output.extend_from_slice(&row.data);
        return Ok(output);
    }
    for index in 0..row.field_count {
        if index != 0 {
            output.put_u8(if format == CopyOutputFormat::Csv {
                b','
            } else {
                b'\t'
            });
        }
        if row.data.remaining() < 4 {
            return Err(user_error("ERROR", "XX000", "truncated COPY field length"));
        }
        let len = row.data.get_i32();
        if len == -1 {
            if format == CopyOutputFormat::Text {
                output.extend_from_slice(b"\\N");
            }
            continue;
        }
        if len < 0 || len as usize > row.data.remaining() {
            return Err(user_error("ERROR", "XX000", "invalid COPY field length"));
        }
        let value = row.data.split_to(len as usize);
        if format == CopyOutputFormat::Csv {
            // Quoting every non-NULL field distinguishes empty strings from NULL.
            output.put_u8(b'"');
            for byte in value {
                if byte == b'"' {
                    output.put_u8(b'"');
                }
                output.put_u8(byte);
            }
            output.put_u8(b'"');
        } else {
            for byte in value {
                match byte {
                    b'\\' => output.extend_from_slice(b"\\\\"),
                    b'\t' => output.extend_from_slice(b"\\t"),
                    b'\n' => output.extend_from_slice(b"\\n"),
                    b'\r' => output.extend_from_slice(b"\\r"),
                    8 => output.extend_from_slice(b"\\b"),
                    12 => output.extend_from_slice(b"\\f"),
                    11 => output.extend_from_slice(b"\\v"),
                    other => output.put_u8(other),
                }
            }
        }
    }
    output.put_u8(b'\n');
    Ok(output)
}

pub(crate) async fn stream_copy_out<C>(
    client: &mut C,
    executor: Arc<dyn Executor>,
    registry: &SessionRegistry,
    sql: &str,
    metrics: &nodus_monitoring::Metrics,
    slow_log: &nodus_monitoring::SlowQueryLog,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<C::Error>,
{
    let (plan, format, header) = nodus_executor::plan_copy_out(sql)
        .map_err(|e| user_error("ERROR", "0A000", e.to_string()))?;
    let plan = transaction_plan(client, plan)?;
    let session_id = session_id_from_client(client);
    let _span = nodus_telemetry::start_span("pgwire.copy_out");
    let _timer = crate::QueryTimer {
        start: std::time::Instant::now(),
        sql,
        session_id: &session_id,
        metrics,
        slow_log,
    };
    registry.set_current_query(&session_id, sql);
    let _guard = CurrentQueryGuard {
        registry,
        session_id: &session_id,
    };
    let ctx = ExecutionContext {
        session_id: session_id.clone(),
        principal_id: principal_id_from_client(client),
        active_roles: vec![],
        authz_catalog_version: 1,
    };
    let wire_format = if format == CopyOutputFormat::Binary {
        Format::UnifiedBinary
    } else {
        Format::UnifiedText
    };
    let stream = start_row_stream(executor, ctx, plan, wire_format).await;
    let mut rows = stream.row_rx;
    let fields = match stream.schema_rx.await {
        Ok(fields) => fields,
        Err(_) => {
            drop(rows);
            let result = join_producer(stream.producer).await?;
            let message = result
                .err()
                .map(|e| e.to_string())
                .unwrap_or_else(|| "COPY produced no schema".into());
            mark_error_status(client);
            return Err(user_error(
                "ERROR",
                sqlstate_for_execution_error(&message),
                message,
            ));
        }
    };
    let count = i16::try_from(fields.len())
        .map_err(|_| user_error("ERROR", "54000", "too many COPY columns"))?;
    let binary = format == CopyOutputFormat::Binary;
    if binary
        && fields
            .iter()
            .any(|field| field.format() != FieldFormat::Binary)
    {
        return Err(user_error(
            "ERROR",
            "0A000",
            "binary COPY encoding is unavailable for an output type",
        ));
    }
    client
        .send(PgWireBackendMessage::CopyOutResponse(CopyOutResponse::new(
            i8::from(binary),
            count,
            vec![i16::from(binary); fields.len()],
        )))
        .await?;
    let mut binary_header = BytesMut::new();
    if binary {
        let mut header = BytesMut::from(&b"PGCOPY\n\xff\r\n\0"[..]);
        header.put_i32(0);
        header.put_i32(0);
        // PostgreSQL puts the header in the first tuple's CopyData message
        // (or with the trailer for an empty result), as Rust's decoder expects.
        binary_header = header;
    } else if header {
        let mut data = BytesMut::new();
        for field in fields.iter() {
            let name = field.name().as_bytes();
            data.put_i32(
                i32::try_from(name.len())
                    .map_err(|_| user_error("ERROR", "54000", "COPY column name too long"))?,
            );
            data.extend_from_slice(name);
        }
        client
            .send(PgWireBackendMessage::CopyData(CopyData::new(
                copy_row(DataRow::new(data, count), format)?.freeze(),
            )))
            .await?;
    }
    let mut total = 0;
    while let Some(row) = rows.recv().await {
        if registry.is_cancelled(&session_id) || registry.is_query_cancelled(&session_id) {
            drop(rows);
            let _ = join_producer(stream.producer).await;
            mark_error_status(client);
            return Err(user_error(
                "ERROR",
                "57014",
                "canceling COPY due to user request",
            ));
        }
        let mut data = copy_row(row, format)?;
        if !binary_header.is_empty() {
            binary_header.extend_from_slice(&data);
            data = binary_header.split();
        }
        client
            .send(PgWireBackendMessage::CopyData(CopyData::new(data.freeze())))
            .await?;
        total += 1;
    }
    if let Err(error) = join_producer(stream.producer).await? {
        mark_error_status(client);
        let message = error.to_string();
        return Err(user_error(
            "ERROR",
            sqlstate_for_execution_error(&message),
            message,
        ));
    }
    if binary {
        let mut trailer = binary_header;
        trailer.put_i16(-1);
        client
            .send(PgWireBackendMessage::CopyData(CopyData::new(
                trailer.freeze(),
            )))
            .await?;
    }
    client
        .send(PgWireBackendMessage::CopyDone(CopyDone::new()))
        .await?;
    client
        .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
            format!("COPY {total}"),
        )))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn text_copy_escapes_values_and_distinguishes_null() {
        let mut bytes = BytesMut::new();
        bytes.put_i32(3);
        bytes.extend_from_slice(b"a\t\\");
        bytes.put_i32(-1);
        bytes.put_i32(0);
        assert_eq!(
            &copy_row(DataRow::new(bytes.clone(), 3), CopyOutputFormat::Text).unwrap()[..],
            b"a\\t\\\\\t\\N\t\n"
        );
        assert_eq!(
            &copy_row(DataRow::new(bytes, 3), CopyOutputFormat::Csv).unwrap()[..],
            b"\"a\t\\\",,\"\"\n"
        );
    }
}
