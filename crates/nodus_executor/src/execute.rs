//! Plan execution: the logical-plan dispatch (`execute_logical_inner` — DDL,
//! DML, SELECT with joins/aggregates/set-ops, transactions, RBAC, and virtual
//! catalog reads) and the physical row pipeline (`execute_physical_inner`).

use crate::aggregates::*;
use crate::*;
use anyhow::Result;
use bytes::Bytes;
use chrono::Utc;
use nodus_audit::{AuditEvent, AuditSink};
use nodus_authz::{Action, AuthzContext, AuthzEngine, AuthzRequest};
use nodus_catalog::{
    AuditEventId, CatalogReader, CatalogWriter, ColumnDescriptor, CreateTableRequest,
    DescriptorState, IndexId, MemoryCatalog, PrincipalId, ResourceRef, RoleId, TableId,
};
use nodus_storage_api::{IntentReplacement, KeyRange, KvEngine, Timestamp, TxnId};
use nodus_txn::TxnManager;
use std::collections::HashMap;
use std::sync::Arc;

impl MemExecutor {
    pub(crate) fn execute_logical_inner(
        &self,
        ctx: &ExecutionContext,
        plan: LogicalPlan,
    ) -> Result<QueryOutput> {
        match plan {
            LogicalPlan::CreateSchema {
                schema_name,
                if_not_exists,
            } => self.exec_create_schema(ctx, schema_name, if_not_exists),
            LogicalPlan::DropSchema {
                schema_name,
                if_exists,
                cascade: _,
            } => self.exec_drop_schema(ctx, schema_name, if_exists),
            LogicalPlan::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
            } => self.exec_create_table(ctx, name, columns, constraints, if_not_exists),
            LogicalPlan::CreateView { name, query } => self.exec_create_view(ctx, name, query),
            LogicalPlan::DropView { name, if_exists } => self.exec_drop_view(ctx, name, if_exists),
            LogicalPlan::DropTable { name, if_exists } => {
                self.exec_drop_table(ctx, name, if_exists)
            }
            LogicalPlan::Insert {
                table_name,
                columns,
                values_list,
                returning,
            } => self.exec_insert(ctx, table_name, columns, values_list, returning),
            LogicalPlan::Select {
                ctes,
                table_name,
                table_alias,
                joins,
                projection,
                group_by,
                filter,
                having,
                order_by,
                limit,
                offset,
                distinct,
            } => self.exec_select(
                ctx,
                ctes,
                table_name,
                table_alias,
                joins,
                projection,
                group_by,
                filter,
                having,
                order_by,
                limit,
                offset,
                distinct,
            ),
            LogicalPlan::Update {
                table_name,
                assignments,
                filter,
                returning,
            } => self.exec_update(ctx, table_name, assignments, filter, returning),
            LogicalPlan::Delete {
                table_name,
                filter,
                returning,
            } => self.exec_delete(ctx, table_name, filter, returning),
            LogicalPlan::AlterTable {
                table_name,
                operation,
            } => self.exec_alter_table(ctx, table_name, operation),
            LogicalPlan::CreateIndex {
                name,
                table_name,
                columns,
                unique,
                if_not_exists,
            } => self.exec_create_index(ctx, name, table_name, columns, unique, if_not_exists),
            LogicalPlan::DropIndex { name, if_exists } => {
                self.exec_drop_index(ctx, name, if_exists)
            }
            LogicalPlan::CreateRole { name } => self.exec_create_role(ctx, name),
            LogicalPlan::Grant {
                privilege,
                object_name,
                grantee,
            } => self.exec_grant(ctx, privilege, object_name, grantee),
            LogicalPlan::Revoke {
                privilege,
                object_name,
                revokee,
            } => self.exec_revoke(ctx, privilege, object_name, revokee),
            LogicalPlan::Begin => self.exec_begin(ctx),
            LogicalPlan::Commit => self.exec_commit(ctx),
            LogicalPlan::Rollback => self.exec_rollback(ctx),
            LogicalPlan::Savepoint { name } => self.exec_savepoint(ctx, name),
            LogicalPlan::RollbackToSavepoint { name } => self.exec_rollback_to_savepoint(ctx, name),
            LogicalPlan::ReleaseSavepoint { name } => self.exec_release_savepoint(ctx, name),
            LogicalPlan::ShowVariable { variable } => self.exec_show_variable(ctx, variable),
            LogicalPlan::SetVariable { variable, value } => {
                self.exec_set_variable(ctx, variable, value)
            }
            LogicalPlan::Noop { tag } => Ok(QueryOutput::tag(&tag)),
            LogicalPlan::SelectLiteral { values } => self.exec_select_literal(values),
            LogicalPlan::SetOp {
                op,
                all,
                left,
                right,
            } => self.exec_set_op(ctx, op, all, left, right),
            LogicalPlan::TableFunction(spec) => self.exec_table_function(spec),
        }
    }

    pub(crate) fn execute_physical_inner(
        &self,
        ctx: &ExecutionContext,
        plan: PhysicalPlan,
    ) -> Result<Vec<Row>> {
        // Retained for the point-get path used by lower layers/tests.
        match plan {
            PhysicalPlan::LocalPointGet { table_id, id } => {
                let read_ts = self.read_ts(&ctx.session_id);
                let key = format!("{}:{}", table_id, id);
                self.maybe_read_barrier(&ctx.session_id, key.as_bytes())?;
                match self.kv.get(key.as_bytes(), read_ts)? {
                    Some(val) => {
                        let row: Vec<Value> = serde_json::from_slice(&val).unwrap_or_default();
                        Ok(vec![Row {
                            values: row.into_iter().collect(),
                        }])
                    }
                    None => Ok(vec![]),
                }
            }
            _ => Ok(vec![]),
        }
    }
}
