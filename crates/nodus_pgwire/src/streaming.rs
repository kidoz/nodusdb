//! Shared executor→socket streaming bridge used by both the simple- and
//! extended-query handlers.
//!
//! The (blocking) executor produces rows into a bounded channel; the async
//! handler drains them to the socket. A full channel back-pressures the producer
//! (it blocks on send), so a slow client throttles the scan instead of buffering
//! the whole result. The schema is handed over once via a oneshot so the handler
//! can emit `RowDescription` before the first row.

use std::sync::Arc;

use nodus_executor::{ExecutionContext, Executor, LogicalPlan, Row as ExecRow, RowSink};
use pgwire::api::portal::Format;
use pgwire::api::results::FieldInfo;
use pgwire::error::PgWireError;
use pgwire::messages::data::DataRow;
use tokio::sync::{mpsc, oneshot};

use crate::encoding::encode_row;
use crate::wire_format::{field_info_for_output, user_error};

/// Bounded in-flight rows between the blocking executor and the socket writer.
/// Small enough to bound memory, large enough to keep the socket busy.
pub(crate) const STREAM_CHANNEL_CAPACITY: usize = 512;

/// A [`RowSink`] that encodes each row on the blocking executor thread and sends
/// it to the socket-writing task through a bounded channel.
struct ChannelSink {
    schema_tx: Option<oneshot::Sender<Arc<Vec<FieldInfo>>>>,
    row_tx: mpsc::Sender<DataRow>,
    /// The client's requested result-column format (text/binary), honored when
    /// building the field descriptors that drive row encoding.
    format: Format,
    field_info: Option<Arc<Vec<FieldInfo>>>,
}

impl RowSink for ChannelSink {
    fn schema(&mut self, columns: Vec<String>, types: Vec<String>) {
        let field_info = field_info_for_output(&columns, &types, |i, _| self.format.format_for(i));
        self.field_info = Some(field_info.clone());
        if let Some(tx) = self.schema_tx.take() {
            // A dropped receiver means the writer went away; the next `row` send
            // will observe it and stop the producer.
            let _ = tx.send(field_info);
        }
    }

    fn row(&mut self, row: ExecRow) -> anyhow::Result<()> {
        let field_info = self
            .field_info
            .clone()
            .ok_or_else(|| anyhow::anyhow!("row produced before schema"))?;
        let data_row = encode_row(&row.values, field_info)?;
        self.row_tx
            .blocking_send(data_row)
            .map_err(|_| anyhow::anyhow!("client went away"))?;
        Ok(())
    }
}

/// A running streaming execution: the schema arrives on `schema_rx` (before the
/// first row), encoded rows on `row_rx`, and the command tag / terminal error
/// from awaiting `producer` once `row_rx` is drained.
pub(crate) struct RowStream {
    pub schema_rx: oneshot::Receiver<Arc<Vec<FieldInfo>>>,
    pub row_rx: mpsc::Receiver<DataRow>,
    pub producer: tokio::task::JoinHandle<anyhow::Result<String>>,
}

/// Spawns the executor on the blocking pool, streaming `plan`'s rows into a
/// bounded channel, encoding each in the client's requested `format`.
pub(crate) fn start_row_stream(
    executor: Arc<dyn Executor>,
    ctx: ExecutionContext,
    plan: LogicalPlan,
    format: Format,
) -> RowStream {
    let (schema_tx, schema_rx) = oneshot::channel::<Arc<Vec<FieldInfo>>>();
    let (row_tx, row_rx) = mpsc::channel::<DataRow>(STREAM_CHANNEL_CAPACITY);
    let producer = tokio::task::spawn_blocking(move || {
        let mut sink = ChannelSink {
            schema_tx: Some(schema_tx),
            row_tx,
            format,
            field_info: None,
        };
        executor.execute_streaming(&ctx, plan, &mut sink)
    });
    RowStream {
        schema_rx,
        row_rx,
        producer,
    }
}

/// Awaits the producer task, turning a join failure (panic/cancel) into a
/// PgWire-level error.
pub(crate) async fn join_producer(
    handle: tokio::task::JoinHandle<anyhow::Result<String>>,
) -> Result<anyhow::Result<String>, PgWireError> {
    handle
        .await
        .map_err(|e| user_error("ERROR", "XX000", format!("execution task failed: {e}")))
}
