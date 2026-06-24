//! COPY (`FROM STDIN` / `TO STDOUT`) protocol handler: tracks streamed row
//! counts and emits the terminal CommandComplete / ReadyForQuery.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{Sink, SinkExt};
use nodus_security::SessionRegistry;
use pgwire::api::copy::CopyHandler;
use pgwire::api::{ClientInfo, PgWireConnectionState};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::response::{CommandComplete, ReadyForQuery};

use crate::client_meta::{mark_error_status, session_id_from_client, tx_status_from_client};
use crate::{METADATA_COPY_EXTENDED, METADATA_COPY_ROWS};

#[derive(Default)]
pub struct NodusCopyHandler {
    pub(crate) registry: Arc<SessionRegistry>,
}

#[async_trait]
impl CopyHandler for NodusCopyHandler {
    async fn on_copy_data<C>(&self, client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let rows = client
            .metadata()
            .get(METADATA_COPY_ROWS)
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        let chunk_rows = copy_data
            .data
            .iter()
            .filter(|b| **b == b'\n')
            .count()
            .max(1);
        client.metadata_mut().insert(
            METADATA_COPY_ROWS.to_owned(),
            rows.saturating_add(chunk_rows).to_string(),
        );
        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let rows = client
            .metadata_mut()
            .remove(METADATA_COPY_ROWS)
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        let extended_copy = client
            .metadata_mut()
            .remove(METADATA_COPY_EXTENDED)
            .as_deref()
            == Some("1");
        let session_id = session_id_from_client(client);
        self.registry.finish_current_query(&session_id);
        client
            .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                format!("COPY {rows}"),
            )))
            .await?;
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
        client.metadata_mut().remove(METADATA_COPY_ROWS);
        client.metadata_mut().remove(METADATA_COPY_EXTENDED);
        let session_id = session_id_from_client(client);
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
