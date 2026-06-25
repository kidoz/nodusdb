//! COPY (`FROM STDIN` / `TO STDOUT`) protocol handler.
//!
//! `FROM STDIN` data is buffered per connection, decoded with the shared
//! `nodus_import` COPY decoder on `CopyDone`, and inserted through the
//! in-process executor — so rows actually persist instead of being counted and
//! dropped. The terminal `CommandComplete` reports the real inserted count.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::{Sink, SinkExt};
use nodus_security::SessionRegistry;
use pgwire::api::copy::CopyHandler;
use pgwire::api::{ClientInfo, PgWireConnectionState};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::response::{CommandComplete, ReadyForQuery};

use crate::client_meta::{
    mark_error_status, principal_id_from_client, session_id_from_client, tx_status_from_client,
};
use crate::{METADATA_COPY_EXTENDED, METADATA_COPY_STMT};

pub struct NodusCopyHandler {
    pub(crate) registry: Arc<SessionRegistry>,
    pub(crate) executor: Arc<dyn nodus_executor::Executor>,
    /// Per-connection raw COPY bytes, keyed by session id, accumulated across
    /// `CopyData` frames and consumed on `CopyDone`/`CopyFail`.
    pub(crate) inflight: Mutex<HashMap<String, Vec<u8>>>,
}

impl NodusCopyHandler {
    pub(crate) fn new(
        registry: Arc<SessionRegistry>,
        executor: Arc<dyn nodus_executor::Executor>,
    ) -> Self {
        Self {
            registry,
            executor,
            inflight: Mutex::new(HashMap::new()),
        }
    }

    /// Decodes the buffered COPY body and inserts it through the executor,
    /// returning the number of rows inserted.
    fn ingest(
        executor: Arc<dyn nodus_executor::Executor>,
        ctx: nodus_executor::ExecutionContext,
        header: String,
        bytes: Vec<u8>,
    ) -> anyhow::Result<usize> {
        let spec = nodus_import::parse_copy_header(&header)?;
        // Wire COPY data is the row stream only (no psql `\.` terminator).
        let body = String::from_utf8_lossy(&bytes);
        let rows = nodus_import::decode_rows(&body, spec.format)?;
        let total = rows.len();
        for chunk in rows.chunks(500) {
            let sql = nodus_import::synthesize_insert(&spec, chunk);
            for stmt in nodus_sql::parse_sql(&sql)? {
                let plan = nodus_executor::plan_statement(&stmt, &[])?;
                executor.execute_logical(&ctx, plan)?;
            }
        }
        Ok(total)
    }
}

#[async_trait]
impl CopyHandler for NodusCopyHandler {
    async fn on_copy_data<C>(&self, client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session_id = session_id_from_client(client);
        let mut inflight = self.inflight.lock().unwrap();
        inflight
            .entry(session_id)
            .or_default()
            .extend_from_slice(&copy_data.data);
        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session_id = session_id_from_client(client);
        let extended_copy = client
            .metadata_mut()
            .remove(METADATA_COPY_EXTENDED)
            .as_deref()
            == Some("1");
        let header = client
            .metadata_mut()
            .remove(METADATA_COPY_STMT)
            .unwrap_or_default();
        let bytes = self
            .inflight
            .lock()
            .unwrap()
            .remove(&session_id)
            .unwrap_or_default();
        let ctx = nodus_executor::ExecutionContext {
            session_id: session_id.clone(),
            principal_id: principal_id_from_client(client),
            active_roles: vec![],
            authz_catalog_version: 1,
        };

        let executor = self.executor.clone();
        let ingested =
            tokio::task::spawn_blocking(move || Self::ingest(executor, ctx, header, bytes))
                .await
                .map_err(|e| {
                    PgWireError::ApiError(Box::new(std::io::Error::other(e.to_string())))
                })?;

        self.registry.finish_current_query(&session_id);

        match ingested {
            Ok(rows) => {
                client
                    .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                        format!("COPY {rows}"),
                    )))
                    .await?;
            }
            Err(e) => {
                mark_error_status(client);
                client
                    .send(PgWireBackendMessage::ErrorResponse(
                        ErrorInfo::new("ERROR".to_owned(), "22P04".to_owned(), e.to_string())
                            .into(),
                    ))
                    .await?;
            }
        }

        if !extended_copy {
            client
                .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                    tx_status_from_client(client),
                )))
                .await?;
        }
        client.flush().await?;
        if extended_copy {
            client.set_state(PgWireConnectionState::AwaitingSync);
        } else {
            client.set_state(PgWireConnectionState::ReadyForQuery);
        }
        Ok(())
    }

    async fn on_copy_fail<C>(&self, client: &mut C, fail: CopyFail) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session_id = session_id_from_client(client);
        self.inflight.lock().unwrap().remove(&session_id);
        client.metadata_mut().remove(METADATA_COPY_EXTENDED);
        client.metadata_mut().remove(METADATA_COPY_STMT);
        self.registry.finish_current_query(&session_id);
        mark_error_status(client);
        let msg = if fail.message.is_empty() {
            "COPY failed".to_owned()
        } else {
            fail.message
        };
        client
            .send(PgWireBackendMessage::ErrorResponse(
                ErrorInfo::new("ERROR".to_owned(), "57014".to_owned(), msg).into(),
            ))
            .await?;
        client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                tx_status_from_client(client),
            )))
            .await?;
        client.flush().await?;
        client.set_state(PgWireConnectionState::ReadyForQuery);
        Ok(())
    }
}
