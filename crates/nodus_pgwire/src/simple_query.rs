//! The simple-query protocol handler: executes a `Query` message end-to-end,
//! streaming RowDescription/DataRow/CommandComplete and handling COPY, SET,
//! transaction control, and metadata introspection.

use std::fmt::Debug;

use async_trait::async_trait;
use futures_util::{Sink, SinkExt, StreamExt, stream};
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{FieldFormat, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireConnectionState};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::copy::CopyDone;
use pgwire::messages::response::{CommandComplete, EmptyQueryResponse, ReadyForQuery};
use tracing::{error, info};

use crate::client_meta::*;
use crate::encoding::*;
use crate::wire_format::*;
use crate::{CurrentQueryGuard, NodusQueryHandler, QueryTimer, execute_off_reactor};
use crate::{METADATA_COPY_EXTENDED, METADATA_COPY_ROWS};

#[async_trait]
impl SimpleQueryHandler for NodusQueryHandler {
    async fn on_query<C>(
        &self,
        client: &mut C,
        query: pgwire::messages::simplequery::Query,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client.set_state(PgWireConnectionState::QueryInProgress);
        let query_string = query.query;
        if query_string.trim().is_empty() || query_string.trim() == ";" {
            client
                .feed(PgWireBackendMessage::EmptyQueryResponse(
                    EmptyQueryResponse::new(),
                ))
                .await?;
        } else if is_copy_from_stdin(&query_string) {
            let session_id = session_id_from_client(client);
            self.registry.set_current_query(&session_id, &query_string);
            client.metadata_mut().extend([
                (METADATA_COPY_ROWS.to_owned(), "0".to_owned()),
                (METADATA_COPY_EXTENDED.to_owned(), "0".to_owned()),
            ]);
            client
                .send(PgWireBackendMessage::CopyInResponse(
                    pgwire::messages::copy::CopyInResponse::new(
                        copy_format_code(&query_string),
                        copy_column_count(&query_string),
                        vec![
                            copy_format_code(&query_string) as i16;
                            copy_column_count(&query_string) as usize
                        ],
                    ),
                ))
                .await?;
            client.flush().await?;
            client.set_state(PgWireConnectionState::CopyInProgress);
            return Ok(());
        } else if is_copy_to_stdout(&query_string) {
            let session_id = session_id_from_client(client);
            self.registry.set_current_query(&session_id, &query_string);
            self.registry.finish_current_query(&session_id);
            client
                .send(PgWireBackendMessage::CopyOutResponse(
                    pgwire::messages::copy::CopyOutResponse::new(
                        copy_format_code(&query_string),
                        copy_column_count(&query_string),
                        vec![
                            copy_format_code(&query_string) as i16;
                            copy_column_count(&query_string) as usize
                        ],
                    ),
                ))
                .await?;
            client
                .send(PgWireBackendMessage::CopyDone(CopyDone::new()))
                .await?;
            client
                .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                    "COPY 0".to_owned(),
                )))
                .await?;
        } else {
            let resp = self.do_query(client, &query_string).await?;
            for r in resp {
                match r {
                    Response::EmptyQuery => {
                        client
                            .feed(PgWireBackendMessage::EmptyQueryResponse(
                                EmptyQueryResponse::new(),
                            ))
                            .await?;
                    }
                    Response::Query(results) => {
                        let row_desc = row_description(&results.row_schema());
                        client
                            .send(PgWireBackendMessage::RowDescription(row_desc))
                            .await?;
                        let command_tag = results.command_tag().to_owned();
                        let mut rows = 0;
                        let mut data_rows = results.data_rows();
                        while let Some(row) = data_rows.next().await {
                            rows += 1;
                            client.feed(PgWireBackendMessage::DataRow(row?)).await?;
                        }
                        client
                            .send(PgWireBackendMessage::CommandComplete(
                                Tag::new(&command_tag).with_rows(rows).into(),
                            ))
                            .await?;
                    }
                    Response::Execution(tag) => {
                        let command: CommandComplete = tag.into();
                        apply_command_tag_to_tx_status(client, &command.tag);
                        client
                            .send(PgWireBackendMessage::CommandComplete(command))
                            .await?;
                    }
                    Response::Error(e) => {
                        mark_error_status(client);
                        client
                            .send(PgWireBackendMessage::ErrorResponse((*e).into()))
                            .await?;
                    }
                    Response::CopyIn(_) | Response::CopyOut(_) | Response::CopyBoth(_) => {
                        return Err(user_error(
                            "ERROR",
                            "0A000",
                            "COPY response is only supported by COPY statements",
                        ));
                    }
                }
            }
        }

        client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                tx_status_from_client(client),
            )))
            .await?;
        client.flush().await?;
        client.set_state(PgWireConnectionState::ReadyForQuery);
        Ok(())
    }

    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        info!("Received query: {}", query);
        self.metrics.queries_total.inc();

        let session_id = {
            let id = session_id_from_client(client);
            if id.is_empty() {
                self.session_id.clone()
            } else {
                id
            }
        };

        // Honor administrative termination and PostgreSQL cancel requests.
        if self.registry.is_cancelled(&session_id) {
            self.metrics.query_errors_total.inc();
            return Err(user_error(
                "ERROR",
                "57P01",
                "session terminated by administrator",
            ));
        }
        if self.registry.is_query_cancelled(&session_id) {
            self.metrics.query_errors_total.inc();
            return Err(user_error(
                "ERROR",
                "57014",
                "canceling statement due to user request",
            ));
        }
        self.registry.set_current_query(&session_id, query);
        let _current_query = CurrentQueryGuard {
            registry: &self.registry,
            session_id: &session_id,
        };

        // OpenTelemetry span covering the statement (no-op unless OTLP is on).
        let _otel_span = nodus_telemetry::start_span("pgwire.simple_query");

        // Times the whole statement regardless of which branch returns.
        let _timer = QueryTimer {
            start: std::time::Instant::now(),
            sql: query,
            session_id: &session_id,
            metrics: &self.metrics,
            slow_log: &self.slow_log,
        };

        // The authenticated principal was stashed in connection metadata by the
        // startup handler; carry it into the execution context for authorization.
        let principal_id = principal_id_from_client(client);

        let ctx = nodus_executor::ExecutionContext {
            session_id: session_id.clone(),
            principal_id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };

        let query_str = query;
        remember_statement_timeout(client, query_str);
        if statement_would_timeout(client, query_str) {
            self.metrics.query_errors_total.inc();
            mark_error_status(client);
            return Err(user_error(
                "ERROR",
                "57014",
                "canceling statement due to statement timeout",
            ));
        }
        if is_set_default_statement(query_str) {
            return Ok(vec![Response::Execution(Tag::new("SET"))]);
        }

        // Parse SQL and translate every statement in a simple-query batch.
        // Drivers such as Npgsql issue startup metadata batches and expect one
        // protocol result for each statement before ReadyForQuery.
        let statements = match nodus_sql::parse_sql(query_str) {
            Ok(stmts) if !stmts.is_empty() => stmts,
            Ok(_) => return Ok(vec![Response::Execution(Tag::new("OK"))]),
            Err(e) => {
                error!("Failed to parse SQL: {}", e);
                self.metrics.query_errors_total.inc();
                let err = ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    format!("Syntax error: {}", e),
                );
                return Err(PgWireError::UserError(Box::new(err)));
            }
        };

        let mut responses = Vec::new();
        for stmt in statements {
            let plan = match nodus_executor::plan_statement(&stmt, &[]) {
                Ok(plan) => plan,
                Err(e) => {
                    error!("Failed to plan SQL: {}", e);
                    self.metrics.query_errors_total.inc();
                    let err = ErrorInfo::new(
                        "ERROR".to_owned(),
                        "0A000".to_owned(),
                        format!("Unsupported feature: {}", e),
                    );
                    return Err(PgWireError::UserError(Box::new(err)));
                }
            };

            let out = match execute_off_reactor(self.executor.clone(), ctx.clone(), plan).await {
                Ok(out) => out,
                Err(e) => {
                    let err_str = e.to_string();
                    let code = sqlstate_for_execution_error(&err_str);
                    let err = ErrorInfo::new("ERROR".to_owned(), code.to_owned(), err_str);
                    mark_error_status(client);
                    return Err(PgWireError::UserError(Box::new(err)));
                }
            };

            if self.registry.is_query_cancelled(&session_id) {
                self.metrics.query_errors_total.inc();
                mark_error_status(client);
                return Err(user_error(
                    "ERROR",
                    "57014",
                    "canceling statement due to user request",
                ));
            }

            // No projected columns => a command tag (CREATE TABLE, INSERT, BEGIN...).
            if out.columns.is_empty() {
                apply_command_tag_to_tx_status(client, &out.tag);
                let tag = command_tag_from_output_tag(&out.tag);
                responses.push(Response::Execution(tag));
                continue;
            }

            // Otherwise a row set: build field descriptors and encode each row.
            let field_info =
                field_info_for_output(&out.columns, &out.types, |_, _| FieldFormat::Text);
            let mut data_rows = Vec::new();
            for row in &out.rows {
                data_rows.push(
                    encode_row(&row.values, field_info.clone()).map_err(PgWireError::IoError),
                );
            }
            responses.push(Response::Query(QueryResponse::new(
                field_info,
                stream::iter(data_rows),
            )));
        }
        Ok(responses)
    }
}
