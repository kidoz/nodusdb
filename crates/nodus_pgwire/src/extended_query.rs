//! The extended-query protocol handler (Parse/Bind/Describe/Execute/Close):
//! prepared-statement and portal handling, cursor materialization, and the
//! per-message execution path.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures_util::{Sink, SinkExt, StreamExt, stream};
use nodus_catalog::PrincipalId;
use nodus_security::SessionRegistry;
use pgwire::api::portal::Portal;
use pgwire::api::query::ExtendedQueryHandler;
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, DEFAULT_NAME, PgWireConnectionState, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::copy::CopyDone;
use pgwire::messages::data::{DataRow, NoData, ParameterDescription};
use pgwire::messages::extendedquery::{
    Bind, BindComplete, Close, CloseComplete, Describe, Execute, Parse, ParseComplete,
    PortalSuspended, Sync as PgSync, TARGET_TYPE_BYTE_PORTAL, TARGET_TYPE_BYTE_STATEMENT,
};
use pgwire::messages::response::{CommandComplete, EmptyQueryResponse, ReadyForQuery};
use postgres_types::Json;
use tracing::{error, info};
use uuid::Uuid;

use crate::client_meta::*;
use crate::encoding::*;
use crate::type_map::*;
use crate::wire_format::*;
use crate::{
    CurrentQueryGuard, METADATA_COPY_EXTENDED, METADATA_COPY_ROWS, QueryTimer, execute_off_reactor,
};

pub struct NodusExtendedQueryHandler {
    pub executor: Arc<dyn nodus_executor::Executor>,
    pub metrics: nodus_monitoring::Metrics,
    pub slow_log: Arc<nodus_monitoring::SlowQueryLog>,
    pub(crate) registry: Arc<SessionRegistry>,
    pub(crate) cursors: RwLock<HashMap<String, PortalCursor>>,
}

pub(crate) struct PortalCursor {
    fields: Arc<Vec<FieldInfo>>,
    rows: Vec<DataRow>,
    position: usize,
    total_rows: usize,
}

impl PortalCursor {
    fn next_chunk(&mut self, max_rows: usize) -> (Vec<DataRow>, bool) {
        let remaining = self.rows.len().saturating_sub(self.position);
        let take = if max_rows == 0 {
            remaining
        } else {
            remaining.min(max_rows)
        };
        let start = self.position;
        let end = start + take;
        self.position = end;
        let suspended = self.position < self.rows.len();
        (self.rows[start..end].to_vec(), suspended)
    }
}

fn cursor_key(session_id: &str, portal_name: &str) -> String {
    format!("{session_id}:{portal_name}")
}

impl NodusExtendedQueryHandler {
    async fn output_metadata_for_query<C>(
        &self,
        client: &C,
        query_str: &str,
    ) -> Vec<(String, String)>
    where
        C: ClientInfo + Sync,
    {
        let session_id = session_id_from_client(client);
        let principal_id = principal_id_from_client(client);
        let ctx = nodus_executor::ExecutionContext {
            session_id,
            principal_id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };

        let Ok(mut stmts) = nodus_sql::parse_sql(query_str) else {
            return Vec::new();
        };
        let Some(parsed) = stmts.pop() else {
            return Vec::new();
        };
        let Ok(plan) = nodus_executor::plan_statement(&parsed, &[]) else {
            return Vec::new();
        };

        let mut plan_zero = plan.clone();
        let can_execute = match &mut plan_zero {
            nodus_executor::LogicalPlan::Select { limit, .. } => {
                *limit = Some(0);
                true
            }
            nodus_executor::LogicalPlan::ShowVariable { .. }
            | nodus_executor::LogicalPlan::SelectLiteral { .. } => true,
            _ => false,
        };
        if !can_execute {
            return Vec::new();
        }

        match execute_off_reactor(self.executor.clone(), ctx, plan_zero).await {
            Ok(out) => out.columns.into_iter().zip(out.types).collect(),
            Err(_) => Vec::new(),
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for NodusExtendedQueryHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser::new())
    }

    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let types = message
            .type_oids
            .iter()
            .map(|oid| Type::from_oid(*oid).unwrap_or(Type::UNKNOWN))
            .collect::<Vec<Type>>();
        let stmt = StoredStatement::new(
            message.name.unwrap_or_else(|| DEFAULT_NAME.to_owned()),
            message.query,
            types,
        );
        client.portal_store().put_statement(Arc::new(stmt));
        client
            .send(PgWireBackendMessage::ParseComplete(ParseComplete::new()))
            .await?;
        Ok(())
    }

    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let statement_name = message.statement_name.as_deref().unwrap_or(DEFAULT_NAME);
        let Some(statement) = client.portal_store().get_statement(statement_name) else {
            return Err(PgWireError::StatementNotFound(statement_name.to_owned()));
        };
        let portal = Portal::try_new(&message, statement)?;
        let key = cursor_key(&session_id_from_client(client), &portal.name);
        self.cursors.write().unwrap().remove(&key);
        client.portal_store().put_portal(Arc::new(portal));
        client
            .send(PgWireBackendMessage::BindComplete(BindComplete::new()))
            .await?;
        Ok(())
    }

    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let portal_name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        let key = cursor_key(&session_id_from_client(client), portal_name);
        let max_rows = message.max_rows as usize;
        let existing = {
            let mut cursors = self.cursors.write().unwrap();
            if let Some(cursor) = cursors.get_mut(&key) {
                let (rows, suspended) = cursor.next_chunk(max_rows);
                let done = !suspended;
                let fields = cursor.fields.clone();
                let total_rows = cursor.total_rows;
                if done {
                    cursors.remove(&key);
                }
                Some((fields, rows, suspended, total_rows))
            } else {
                None
            }
        };
        if let Some((fields, rows, suspended, total_rows)) = existing {
            for row in rows {
                client.send(PgWireBackendMessage::DataRow(row)).await?;
            }
            if suspended {
                client
                    .send(PgWireBackendMessage::PortalSuspended(PortalSuspended::new()))
                    .await?;
            } else {
                client
                    .send(PgWireBackendMessage::CommandComplete(
                        Tag::new("SELECT").with_rows(total_rows).into(),
                    ))
                    .await?;
            }
            let _ = fields;
            return Ok(());
        }

        let Some(portal) = client.portal_store().get_portal(portal_name) else {
            return Err(PgWireError::PortalNotFound(portal_name.to_owned()));
        };

        if is_copy_from_stdin(&portal.statement.statement) {
            let session_id = session_id_from_client(client);
            self.registry
                .set_current_query(&session_id, &portal.statement.statement);
            client.metadata_mut().extend([
                (METADATA_COPY_ROWS.to_owned(), "0".to_owned()),
                (METADATA_COPY_EXTENDED.to_owned(), "1".to_owned()),
            ]);
            client
                .send(PgWireBackendMessage::CopyInResponse(
                    pgwire::messages::copy::CopyInResponse::new(
                        copy_format_code(&portal.statement.statement),
                        copy_column_count(&portal.statement.statement),
                        vec![
                            copy_format_code(&portal.statement.statement) as i16;
                            copy_column_count(&portal.statement.statement) as usize
                        ],
                    ),
                ))
                .await?;
            client.flush().await?;
            client.set_state(PgWireConnectionState::CopyInProgress);
            return Ok(());
        }
        if is_copy_to_stdout(&portal.statement.statement) {
            let session_id = session_id_from_client(client);
            self.registry
                .set_current_query(&session_id, &portal.statement.statement);
            self.registry.finish_current_query(&session_id);
            client
                .send(PgWireBackendMessage::CopyOutResponse(
                    pgwire::messages::copy::CopyOutResponse::new(
                        copy_format_code(&portal.statement.statement),
                        copy_column_count(&portal.statement.statement),
                        vec![
                            copy_format_code(&portal.statement.statement) as i16;
                            copy_column_count(&portal.statement.statement) as usize
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
            return Ok(());
        }

        let returning_statement = portal
            .statement
            .statement
            .to_ascii_uppercase()
            .contains("RETURNING");
        let pgjdbc_generated_key_returning =
            returning_statement && portal.statement.statement.contains('"');
        let should_send_execute_row_description = pgjdbc_generated_key_returning
            || (returning_statement
                && !client
                    .metadata()
                    .contains_key(&described_statement_key(&portal.statement.statement))
                && !client
                    .metadata()
                    .contains_key(&described_portal_key(portal_name)));

        match self.do_query(client, portal.as_ref(), max_rows).await? {
            Response::EmptyQuery => {
                client
                    .send(PgWireBackendMessage::EmptyQueryResponse(
                        EmptyQueryResponse::new(),
                    ))
                    .await?;
            }
            Response::Query(results) => {
                let fields = results.row_schema();
                let mut rows = Vec::new();
                let mut data_rows = results.data_rows();
                while let Some(row) = data_rows.next().await {
                    rows.push(row?);
                }
                if should_send_execute_row_description {
                    client
                        .send(PgWireBackendMessage::RowDescription(row_description(
                            &fields,
                        )))
                        .await?;
                }
                let mut cursor = PortalCursor {
                    fields,
                    rows,
                    position: 0,
                    total_rows: 0,
                };
                cursor.total_rows = cursor.rows.len();
                let (chunk, suspended) = cursor.next_chunk(max_rows);
                for row in chunk {
                    client.send(PgWireBackendMessage::DataRow(row)).await?;
                }
                if suspended {
                    self.cursors.write().unwrap().insert(key, cursor);
                    client
                        .send(PgWireBackendMessage::PortalSuspended(PortalSuspended::new()))
                        .await?;
                } else {
                    client
                        .send(PgWireBackendMessage::CommandComplete(
                            Tag::new("SELECT").with_rows(cursor.position).into(),
                        ))
                        .await?;
                }
            }
            Response::Execution(tag) => {
                let command: CommandComplete = tag.into();
                apply_command_tag_to_tx_status(client, &command.tag);
                client
                    .send(PgWireBackendMessage::CommandComplete(command))
                    .await?;
            }
            Response::Error(err) => {
                mark_error_status(client);
                client
                    .send(PgWireBackendMessage::ErrorResponse((*err).into()))
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
        Ok(())
    }

    async fn on_sync<C>(&self, client: &mut C, _message: PgSync) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                tx_status_from_client(client),
            )))
            .await?;
        client.flush().await?;
        client.set_state(PgWireConnectionState::ReadyForQuery);
        Ok(())
    }

    async fn on_close<C>(&self, client: &mut C, message: Close) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        match message.target_type {
            TARGET_TYPE_BYTE_STATEMENT => {
                client.portal_store().rm_statement(name);
            }
            TARGET_TYPE_BYTE_PORTAL => {
                client.portal_store().rm_portal(name);
                let key = cursor_key(&session_id_from_client(client), name);
                self.cursors.write().unwrap().remove(&key);
            }
            _ => {}
        }
        client
            .send(PgWireBackendMessage::CloseComplete(CloseComplete::new()))
            .await?;
        Ok(())
    }

    /// Overrides the default so a parameterized statement that returns no rows
    /// (INSERT/UPDATE/DELETE/DDL) answers `Describe` with `NoData` instead of
    /// an empty `RowDescription`. pgwire's `send_describe_response` only emits
    /// `NoData` when there are no parameters either, and an empty
    /// `RowDescription` makes pgjdbc treat the statement as result-returning,
    /// discarding its batch update counts.
    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        match message.target_type {
            TARGET_TYPE_BYTE_STATEMENT => {
                if let Some(stmt) = client.portal_store().get_statement(name) {
                    client
                        .metadata_mut()
                        .insert(described_statement_key(&stmt.statement), "1".to_owned());
                    let resp = self.do_describe_statement(client, &stmt).await?;
                    if resp.fields.is_empty() {
                        client
                            .send(PgWireBackendMessage::ParameterDescription(
                                ParameterDescription::new(
                                    resp.parameters.iter().map(|t| t.oid()).collect(),
                                ),
                            ))
                            .await?;
                        client
                            .send(PgWireBackendMessage::NoData(NoData::new()))
                            .await?;
                    } else {
                        client
                            .send(PgWireBackendMessage::ParameterDescription(
                                ParameterDescription::new(
                                    resp.parameters.iter().map(|t| t.oid()).collect(),
                                ),
                            ))
                            .await?;
                        let metadata = self
                            .output_metadata_for_query(client, &stmt.statement)
                            .await;
                        let row_desc = if metadata.is_empty() {
                            row_description(&resp.fields)
                        } else {
                            row_description_from_metadata(&metadata, |_, _| FieldFormat::Text)
                        };
                        client
                            .send(PgWireBackendMessage::RowDescription(row_desc))
                            .await?;
                    }
                } else {
                    return Err(PgWireError::StatementNotFound(name.to_owned()));
                }
            }
            TARGET_TYPE_BYTE_PORTAL => {
                if let Some(portal) = client.portal_store().get_portal(name) {
                    client
                        .metadata_mut()
                        .insert(described_portal_key(name), "1".to_owned());
                    let resp = self.do_describe_portal(client, &portal).await?;
                    if resp.fields.is_empty() {
                        client
                            .send(PgWireBackendMessage::NoData(NoData::new()))
                            .await?;
                    } else {
                        let metadata = self
                            .output_metadata_for_query(client, &portal.statement.statement)
                            .await;
                        let row_desc = if metadata.is_empty() {
                            row_description(&resp.fields)
                        } else {
                            row_description_from_metadata(&metadata, |i, ty| {
                                effective_result_format(
                                    ty,
                                    portal.result_column_format.format_for(i),
                                )
                            })
                        };
                        client
                            .send(PgWireBackendMessage::RowDescription(row_desc))
                            .await?;
                    }
                } else {
                    return Err(PgWireError::PortalNotFound(name.to_owned()));
                }
            }
            _ => return Err(PgWireError::InvalidTargetType(message.target_type)),
        }
        Ok(())
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let raw_sql = &portal.statement.statement;
        info!("Received extended query: {}", raw_sql);
        self.metrics.queries_total.inc();

        let session_id = session_id_from_client(client);
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
        self.registry.set_current_query(&session_id, raw_sql);
        let _current_query = CurrentQueryGuard {
            registry: &self.registry,
            session_id: &session_id,
        };

        let _timer = QueryTimer {
            start: std::time::Instant::now(),
            sql: raw_sql,
            session_id: &session_id,
            metrics: &self.metrics,
            slow_log: &self.slow_log,
        };

        let principal_id = principal_id_from_client(client);

        let ctx = nodus_executor::ExecutionContext {
            session_id: session_id.clone(),
            principal_id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };

        // Extract parameters from the portal natively into Vec<nodus_executor::Value>
        let len = portal.parameter_len();
        let mut params = Vec::with_capacity(len);
        for i in 0..len {
            let param_type = portal
                .statement
                .parameter_types
                .get(i)
                .unwrap_or(&Type::UNKNOWN);

            let format = portal.parameter_format.format_for(i);

            let val = if portal.parameters.get(i).is_none_or(|p| p.is_none()) {
                nodus_executor::Value::Null
            } else if format == pgwire::api::results::FieldFormat::Text {
                let bytes = portal.parameters.get(i).unwrap().as_ref().unwrap();
                let s = String::from_utf8_lossy(bytes).into_owned();
                text_parameter_value(param_type, s)
            } else {
                match *param_type {
                    Type::BOOL => {
                        let v = portal.parameter::<bool>(i, param_type)?.unwrap_or_default();
                        nodus_executor::Value::Bool(v)
                    }
                    Type::INT2 => {
                        let v = portal.parameter::<i16>(i, param_type)?.unwrap_or_default();
                        nodus_executor::Value::Int(v as i64)
                    }
                    Type::INT4 => {
                        let v = portal.parameter::<i32>(i, param_type)?.unwrap_or_default();
                        nodus_executor::Value::Int(v as i64)
                    }
                    Type::INT8 => {
                        let v = portal.parameter::<i64>(i, param_type)?.unwrap_or_default();
                        nodus_executor::Value::Int(v)
                    }
                    Type::FLOAT4 => {
                        let v = portal.parameter::<f32>(i, param_type)?.unwrap_or_default();
                        nodus_executor::Value::Float(v as f64)
                    }
                    Type::FLOAT8 => {
                        let v = portal.parameter::<f64>(i, param_type)?.unwrap_or_default();
                        nodus_executor::Value::Float(v)
                    }
                    Type::NUMERIC => {
                        let bytes = portal.parameters.get(i).unwrap().as_ref().unwrap();
                        let s = String::from_utf8_lossy(bytes).into_owned();
                        text_parameter_value(param_type, s)
                    }
                    Type::OID => {
                        let v = portal.parameter::<u32>(i, param_type)?.unwrap_or_default();
                        nodus_executor::Value::Int(v as i64)
                    }
                    Type::TEXT | Type::VARCHAR => {
                        let v = portal
                            .parameter::<String>(i, param_type)?
                            .unwrap_or_default();
                        nodus_executor::Value::Text(v)
                    }
                    Type::NAME | Type::BPCHAR => {
                        let v = portal
                            .parameter::<String>(i, param_type)?
                            .unwrap_or_default();
                        nodus_executor::Value::Text(v)
                    }
                    Type::BYTEA => {
                        let v = portal
                            .parameter::<Vec<u8>>(i, param_type)?
                            .unwrap_or_default();
                        nodus_executor::Value::Text(format!("\\x{}", hex_encode(&v)))
                    }
                    Type::DATE => {
                        let v = portal
                            .parameter::<NaiveDate>(i, param_type)?
                            .map(|v| v.to_string())
                            .unwrap_or_default();
                        nodus_executor::Value::Text(v)
                    }
                    Type::TIME => {
                        let v = portal
                            .parameter::<NaiveTime>(i, param_type)?
                            .map(|v| v.format("%H:%M:%S%.f").to_string())
                            .unwrap_or_default();
                        nodus_executor::Value::Text(v)
                    }
                    Type::TIMESTAMP => {
                        let v = portal
                            .parameter::<NaiveDateTime>(i, param_type)?
                            .map(|v| v.format("%Y-%m-%d %H:%M:%S%.f").to_string())
                            .unwrap_or_default();
                        nodus_executor::Value::Text(v)
                    }
                    Type::TIMESTAMPTZ => {
                        let v = portal
                            .parameter::<DateTime<Utc>>(i, param_type)?
                            .map(|v| v.to_rfc3339())
                            .unwrap_or_default();
                        nodus_executor::Value::Text(v)
                    }
                    Type::UUID => {
                        let v = portal
                            .parameter::<Uuid>(i, param_type)?
                            .map(|v| v.to_string())
                            .unwrap_or_default();
                        nodus_executor::Value::Text(v)
                    }
                    Type::JSON | Type::JSONB => {
                        let v = portal
                            .parameter::<Json<serde_json::Value>>(i, param_type)?
                            .map(|v| v.0)
                            .unwrap_or(serde_json::Value::Null);
                        nodus_executor::Value::Jsonb(v)
                    }
                    Type::TEXT_ARRAY
                    | Type::VARCHAR_ARRAY
                    | Type::BPCHAR_ARRAY
                    | Type::NAME_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<String>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, nodus_executor::Value::Text)
                    }
                    Type::INT2_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<i16>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |n| nodus_executor::Value::Int(n as i64))
                    }
                    Type::INT4_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<i32>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |n| nodus_executor::Value::Int(n as i64))
                    }
                    Type::INT8_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<i64>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, nodus_executor::Value::Int)
                    }
                    Type::FLOAT4_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<f32>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |n| nodus_executor::Value::Float(n as f64))
                    }
                    Type::FLOAT8_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<f64>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, nodus_executor::Value::Float)
                    }
                    Type::BOOL_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<bool>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, nodus_executor::Value::Bool)
                    }
                    Type::UUID_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<Uuid>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |uuid| nodus_executor::Value::Text(uuid.to_string()))
                    }
                    Type::JSON_ARRAY | Type::JSONB_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<Json<serde_json::Value>>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |json| nodus_executor::Value::Jsonb(json.0))
                    }
                    Type::BYTEA_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<Vec<u8>>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |bytes| {
                            nodus_executor::Value::Text(format!("\\x{}", hex_encode(&bytes)))
                        })
                    }
                    Type::DATE_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<NaiveDate>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |date| nodus_executor::Value::Text(date.to_string()))
                    }
                    Type::TIME_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<NaiveTime>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |time| {
                            nodus_executor::Value::Text(time.format("%H:%M:%S%.f").to_string())
                        })
                    }
                    Type::TIMESTAMP_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<NaiveDateTime>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |ts| {
                            nodus_executor::Value::Text(
                                ts.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
                            )
                        })
                    }
                    Type::TIMESTAMPTZ_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<DateTime<Utc>>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |ts| nodus_executor::Value::Text(ts.to_rfc3339()))
                    }
                    _ => {
                        let v = portal
                            .parameter::<String>(i, &Type::TEXT)
                            .unwrap_or(Some("".to_string()))
                            .unwrap_or_default();
                        nodus_executor::Value::Text(v)
                    }
                }
            };
            params.push(val);
        }

        let query_str = raw_sql;
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
            return Ok(Response::Execution(Tag::new("SET")));
        }

        let stmt = match nodus_sql::parse_sql(query_str) {
            Ok(mut stmts) if !stmts.is_empty() => stmts.remove(0),
            Ok(_) => return Ok(Response::Execution(Tag::new("OK"))),
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
        let plan = match nodus_executor::plan_statement(&stmt, &params) {
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

        if out.columns.is_empty() {
            apply_command_tag_to_tx_status(client, &out.tag);
            let tag = command_tag_from_output_tag(&out.tag);
            return Ok(Response::Execution(tag));
        }

        let field_info = field_info_for_output(&out.columns, &out.types, |i, _| {
            portal.result_column_format.format_for(i)
        });
        let mut data_rows = Vec::new();
        for row in &out.rows {
            data_rows
                .push(encode_row(&row.values, field_info.clone()).map_err(PgWireError::IoError));
        }
        let response = QueryResponse::new(field_info, stream::iter(data_rows));
        Ok(Response::Query(response))
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut param_types = stmt.parameter_types.clone();
        if param_types.is_empty() {
            let mut max_param = 0;
            let query = &stmt.statement;
            for i in 1..=100 {
                let placeholder = format!("${}", i);
                if query.contains(&placeholder) {
                    max_param = i;
                }
            }
            if max_param > 0 {
                param_types = vec![Type::UNKNOWN; max_param];
            }
        }

        let session_id = client
            .metadata()
            .get("nodus_session_id")
            .cloned()
            .unwrap_or_default();
        let principal_id = client
            .metadata()
            .get("nodus_principal_id")
            .and_then(|s| Uuid::parse_str(s).ok())
            .map(PrincipalId)
            .unwrap_or_default();
        let ctx = nodus_executor::ExecutionContext {
            session_id,
            principal_id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };

        let query_str = stmt.statement.as_str();

        let mut fields = vec![];
        if let Ok(mut stmts) = nodus_sql::parse_sql(query_str)
            && let Some(parsed) = stmts.pop()
            && let Ok(plan) = nodus_executor::plan_statement(&parsed, &[])
        {
            let mut plan_zero = plan.clone();
            let mut can_execute = false;
            if let nodus_executor::LogicalPlan::Select { ref mut limit, .. } = plan_zero {
                *limit = Some(0);
                can_execute = true;
            } else if let nodus_executor::LogicalPlan::Insert {
                ref mut values_list,
                ref returning,
                ..
            } = plan_zero
            {
                if !returning.is_empty() {
                    values_list.clear();
                    can_execute = true;
                }
            } else if let nodus_executor::LogicalPlan::ShowVariable { .. } = plan_zero {
                can_execute = true;
            } else if let nodus_executor::LogicalPlan::SelectLiteral { .. } = plan_zero {
                can_execute = true;
            }

            let described = if can_execute {
                execute_off_reactor(self.executor.clone(), ctx.clone(), plan_zero)
                    .await
                    .ok()
            } else {
                None
            };
            if let Some(out) = described {
                for (col_name, col_type) in out.columns.into_iter().zip(out.types) {
                    fields.push(FieldInfo::new(
                        col_name,
                        None,
                        None,
                        map_type(&col_type),
                        FieldFormat::Text,
                    ));
                }
            }
        }

        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query_str = portal.statement.statement.as_str();

        let session_id = client
            .metadata()
            .get("nodus_session_id")
            .cloned()
            .unwrap_or_default();
        let principal_id = client
            .metadata()
            .get("nodus_principal_id")
            .and_then(|s| Uuid::parse_str(s).ok())
            .map(PrincipalId)
            .unwrap_or_default();
        let ctx = nodus_executor::ExecutionContext {
            session_id,
            principal_id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };

        let mut fields = vec![];
        if let Ok(mut stmts) = nodus_sql::parse_sql(query_str)
            && let Some(parsed) = stmts.pop()
            && let Ok(plan) = nodus_executor::plan_statement(&parsed, &[])
        {
            let mut plan_zero = plan.clone();
            let mut can_execute = false;
            if let nodus_executor::LogicalPlan::Select { ref mut limit, .. } = plan_zero {
                *limit = Some(0);
                can_execute = true;
            } else if let nodus_executor::LogicalPlan::Insert {
                ref mut values_list,
                ref returning,
                ..
            } = plan_zero
            {
                if !returning.is_empty() {
                    values_list.clear();
                    can_execute = true;
                }
            } else if let nodus_executor::LogicalPlan::ShowVariable { .. } = plan_zero {
                can_execute = true;
            } else if let nodus_executor::LogicalPlan::SelectLiteral { .. } = plan_zero {
                can_execute = true;
            }

            let described = if can_execute {
                execute_off_reactor(self.executor.clone(), ctx.clone(), plan_zero)
                    .await
                    .ok()
            } else {
                None
            };
            if let Some(out) = described {
                for (col_name, col_type) in out.columns.into_iter().zip(out.types) {
                    fields.push(FieldInfo::new(
                        col_name,
                        None,
                        None,
                        map_type(&col_type),
                        FieldFormat::Text,
                    ));
                }
            }
        }

        Ok(DescribePortalResponse::new(fields))
    }
}
