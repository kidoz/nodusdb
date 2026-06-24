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
            } => {
                let db = self.catalog_reader.get_database("default")?;
                self.authorize(ctx, Action::CreateSchema, ResourceRef::Database(db.id))?;
                match self
                    .catalog_writer
                    .create_schema(nodus_catalog::CreateSchemaRequest {
                        id: nodus_catalog::SchemaId::new(),
                        database_id: db.id,
                        name: schema_name,
                        owner_role_id: None,
                        managed_access: false,
                    }) {
                    Ok(_) => Ok(QueryOutput::tag("CREATE SCHEMA")),
                    Err(e) => {
                        if if_not_exists && e.to_string().contains("already exists") {
                            Ok(QueryOutput::tag("CREATE SCHEMA"))
                        } else {
                            Err(anyhow::anyhow!(e))
                        }
                    }
                }
            }
            LogicalPlan::DropSchema {
                schema_name,
                if_exists,
                cascade: _,
            } => {
                let db_name = "default";
                match self.catalog_reader.get_schema(db_name, &schema_name) {
                    Ok(sch) => {
                        self.authorize(ctx, Action::CreateSchema, ResourceRef::Schema(sch.id))?;
                        self.catalog_writer.drop_schema(sch.id)?;
                        Ok(QueryOutput::tag("DROP SCHEMA"))
                    }
                    Err(e) => {
                        if if_exists {
                            Ok(QueryOutput::tag("DROP SCHEMA"))
                        } else {
                            Err(anyhow::anyhow!(e))
                        }
                    }
                }
            }
            LogicalPlan::CreateTable {
                name,
                columns,
                constraints,
            } => {
                let (db_name, schema_name, table_only) = parse_object_name(&name)?;
                let db = self.catalog_reader.get_database(db_name)?;
                let sch = self.catalog_reader.get_schema(db_name, schema_name)?;
                self.authorize(ctx, Action::CreateTable, ResourceRef::Schema(sch.id))?;
                let descriptors: Vec<_> = columns
                    .iter()
                    .map(|c| ColumnDescriptor {
                        id: nodus_catalog::ColumnId::new(),
                        name: c.name.clone(),
                        version: 1,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        state: DescriptorState::Public,
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                    })
                    .collect();

                let mut unique_cols = Vec::new();
                for (c, d) in columns.iter().zip(descriptors.iter()) {
                    if c.unique {
                        unique_cols.push((d.clone(), c.primary));
                    }
                }

                let tbl = self.catalog_writer.create_table(CreateTableRequest {
                    id: nodus_catalog::TableId::new(),
                    database_id: db.id,
                    schema_id: sch.id,
                    name: table_only.to_string(),
                    columns: descriptors,
                    constraints,
                    view_query: None,
                })?;

                for (col, primary) in unique_cols {
                    let index = nodus_catalog::IndexDescriptor {
                        id: nodus_catalog::IndexId::new(),
                        name: if primary {
                            format!("{}_pkey", table_only)
                        } else {
                            format!("{}_{}_idx", name, col.name)
                        },
                        version: 1,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        state: DescriptorState::Public,
                        index_type: if primary {
                            nodus_catalog::IndexType::Primary
                        } else {
                            nodus_catalog::IndexType::Unique
                        },
                        index_state: nodus_catalog::IndexState::Ready,
                        key_columns: vec![nodus_catalog::IndexColumn {
                            column_id: col.id,
                            descending: false,
                        }],
                        include_columns: vec![],
                        unique: true,
                        global: false,
                        predicate: None,
                        expressions: vec![],
                    };
                    self.catalog_writer.update_table_descriptor(
                        nodus_catalog::TableDescriptorChange::AddIndex {
                            table_id: tbl.id,
                            index,
                        },
                    )?;
                }

                Ok(QueryOutput::tag("CREATE TABLE"))
            }
            LogicalPlan::CreateView { name, query } => {
                let (db_name, schema_name, view_only) = parse_object_name(&name)?;
                let db = self.catalog_reader.get_database(db_name)?;
                let sch = self.catalog_reader.get_schema(db_name, schema_name)?;
                self.authorize(ctx, Action::CreateTable, ResourceRef::Schema(sch.id))?;

                // Resolve the schema of the view by planning/executing a dummy pass or just full execute
                // For MVP, we can just execute the query and take its output columns.
                let mut is_valid = false;
                let mut view_cols = Vec::new();

                // Hack: serialize the logical plan to store it
                let view_query_json = serde_json::to_string(&*query)?;

                // Run query to get shape
                if let Ok(out) = self.execute_logical_inner(ctx, *query) {
                    for (i, cname) in out.columns.iter().enumerate() {
                        let ty = out.types.get(i).unwrap_or(&"VARCHAR".to_string()).clone();
                        view_cols.push(ColumnDescriptor {
                            id: nodus_catalog::ColumnId::new(),
                            name: cname.clone(),
                            version: 1,
                            created_at: Utc::now(),
                            updated_at: Utc::now(),
                            state: DescriptorState::Public,
                            data_type: ty,
                            nullable: true,
                        });
                    }
                    is_valid = true;
                }

                if !is_valid {
                    anyhow::bail!("Failed to resolve view query schema");
                }

                self.catalog_writer.create_table(CreateTableRequest {
                    id: nodus_catalog::TableId::new(),
                    database_id: db.id,
                    schema_id: sch.id,
                    name: view_only.to_string(),
                    columns: view_cols,
                    constraints: vec![],
                    view_query: Some(view_query_json),
                })?;

                Ok(QueryOutput::tag("CREATE VIEW"))
            }
            LogicalPlan::DropView { name, if_exists } => {
                let (db_name, schema_name, view_only) = parse_object_name(&name)?;
                match self
                    .catalog_reader
                    .get_table(db_name, schema_name, view_only)
                {
                    Ok(tbl) => {
                        self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
                        if tbl.view_query.is_none() {
                            anyhow::bail!("{} is not a view", name);
                        }
                        self.catalog_writer.drop_table(tbl.id)?;
                        Ok(QueryOutput::tag("DROP VIEW"))
                    }
                    Err(e) => {
                        if if_exists {
                            Ok(QueryOutput::tag("DROP VIEW"))
                        } else {
                            Err(anyhow::anyhow!(e))
                        }
                    }
                }
            }
            LogicalPlan::DropTable { name, if_exists } => {
                let (db_name, schema_name, table_only) = parse_object_name(&name)?;
                match self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)
                {
                    Ok(tbl) => {
                        self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
                        self.catalog_writer.drop_table(tbl.id)?;
                        Ok(QueryOutput::tag("DROP TABLE"))
                    }
                    Err(e) => {
                        if if_exists {
                            Ok(QueryOutput::tag("DROP TABLE"))
                        } else {
                            Err(anyhow::anyhow!(e))
                        }
                    }
                }
            }
            LogicalPlan::Insert {
                table_name,
                columns,
                values_list,

                returning,
            } => {
                let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                self.authorize(ctx, Action::Insert, ResourceRef::Table(tbl.id))?;

                let col_names: Vec<&str> = tbl.columns.iter().map(|c| c.name.as_str()).collect();
                let mut inserted_count = 0;
                let mut returning_rows = Vec::new();

                for values in values_list {
                    // Build the Values row in table-column order...
                    let mut raw = vec![Value::Null; col_names.len()];
                    if columns.is_empty() {
                        for (i, v) in values.iter().enumerate() {
                            if i < raw.len() {
                                raw[i] = v.clone();
                            }
                        }
                    } else {
                        for (cname, val) in columns.iter().zip(values.iter()) {
                            if let Some(idx) = col_names.iter().position(|c| c == cname) {
                                raw[idx] = val.clone();
                            }
                        }
                    }
                    // ...then coerce each cell to its column's type if it's Text, otherwise assume it's correctly bound.
                    let mut row = Vec::new();
                    for (i, c) in tbl.columns.iter().enumerate() {
                        let val = match &raw[i] {
                            Value::Text(s) => coerce(s, column_type(&c.data_type)),
                            other => other.clone(),
                        };
                        if !c.nullable && val == Value::Null {
                            anyhow::bail!("Column {} cannot be NULL", c.name);
                        }
                        row.push(val);
                    }

                    self.check_unique_constraints(&ctx.session_id, &tbl, &row, None)?;
                    self.check_table_constraints(
                        ctx,
                        &tbl,
                        &row,
                        &col_names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                    )?;

                    // Primary key = first column's rendered value.
                    let pk = row.first().map(render).unwrap_or_default();
                    let key = format!("{}:{}", tbl.id, pk);
                    let encoded = serde_json::to_string(&row)?;
                    self.write_row(&ctx.session_id, key, encoded)?;

                    // Maintain secondary indexes.
                    for idx in &tbl.indexes {
                        for kcol in &idx.key_columns {
                            if let Some(pos) =
                                tbl.columns.iter().position(|c| c.id == kcol.column_id)
                            {
                                let index_val = row.get(pos).unwrap_or(&Value::Null);
                                self.write_index_entry(&ctx.session_id, idx.id, index_val, &pk)?;
                            }
                        }
                    }

                    inserted_count += 1;
                    if !returning.is_empty() {
                        returning_rows.push(row);
                    }
                }

                if returning.is_empty() {
                    Ok(QueryOutput::tag(&format!("INSERT 0 {}", inserted_count)))
                } else {
                    let col_names: Vec<&str> =
                        tbl.columns.iter().map(|c| c.name.as_str()).collect();
                    let indices: Vec<Option<usize>> = returning
                        .iter()
                        .map(|c| {
                            col_names
                                .iter()
                                .position(|&tc| tc == c || tc.ends_with(&format!(".{}", c)))
                        })
                        .collect();
                    let rows = returning_rows
                        .into_iter()
                        .map(|r| Row {
                            values: indices
                                .iter()
                                .map(|i| {
                                    i.and_then(|idx| r.get(idx)).cloned().unwrap_or(Value::Null)
                                })
                                .collect(),
                        })
                        .collect();
                    Ok(QueryOutput {
                        tag: format!("INSERT 0 {}", inserted_count),
                        columns: returning.clone(),
                        types: Self::returning_types(&tbl.columns, &returning),
                        rows,
                    })
                }
            }
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
            } => {
                let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                self.authorize(ctx, Action::Update, ResourceRef::Table(tbl.id))?;
                let col_names: Vec<&str> = tbl.columns.iter().map(|c| c.name.as_str()).collect();

                let mut updated = 0;
                let mut returning_rows = Vec::new();
                for mut row in self.scan_rows(tbl.id, &ctx.session_id)? {
                    if !self.row_matches(ctx, &row, &tbl.columns, filter.as_ref()) {
                        continue;
                    }
                    let old_row = row.clone();
                    let old_pk_str = old_row.first().map(render).unwrap_or_default();
                    let old_key = format!("{}:{}", tbl.id, old_pk_str);
                    for (col, val) in &assignments {
                        if let Some(idx) = col_names.iter().position(|c| c == col) {
                            let coerced = match val {
                                Value::Text(s) => {
                                    coerce(s, column_type(&tbl.columns[idx].data_type))
                                }
                                other => other.clone(),
                            };
                            if !tbl.columns[idx].nullable && coerced == Value::Null {
                                anyhow::bail!("Column {} cannot be NULL", col);
                            }
                            row[idx] = coerced;
                        }
                    }

                    let pk_str = row.first().map(render).unwrap_or_default();
                    self.check_unique_constraints(&ctx.session_id, &tbl, &row, Some(&pk_str))?;
                    self.check_table_constraints(
                        ctx,
                        &tbl,
                        &row,
                        &col_names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                    )?;

                    let new_key = format!("{}:{}", tbl.id, pk_str);
                    self.write_row(
                        &ctx.session_id,
                        new_key.clone(),
                        serde_json::to_string(&row)?,
                    )?;
                    if new_key != old_key {
                        self.delete_row(&ctx.session_id, old_key)?;
                    }

                    // Maintain secondary indexes.
                    for idx in &tbl.indexes {
                        for kcol in &idx.key_columns {
                            if let Some(pos) =
                                tbl.columns.iter().position(|c| c.id == kcol.column_id)
                            {
                                let old_index_val = old_row.get(pos).unwrap_or(&Value::Null);
                                let new_index_val = row.get(pos).unwrap_or(&Value::Null);
                                if old_index_val != new_index_val || old_pk_str != pk_str {
                                    self.delete_index_entry(
                                        &ctx.session_id,
                                        idx.id,
                                        old_index_val,
                                        &old_pk_str,
                                    )?;
                                    self.write_index_entry(
                                        &ctx.session_id,
                                        idx.id,
                                        new_index_val,
                                        &pk_str,
                                    )?;
                                }
                            }
                        }
                    }

                    updated += 1;
                    if !returning.is_empty() {
                        returning_rows.push(row);
                    }
                }
                if returning.is_empty() {
                    Ok(QueryOutput::tag(&format!("UPDATE {updated}")))
                } else {
                    let indices: Vec<Option<usize>> = returning
                        .iter()
                        .map(|c| {
                            col_names
                                .iter()
                                .position(|&tc| tc == c || tc.ends_with(&format!(".{}", c)))
                        })
                        .collect();
                    let rows = returning_rows
                        .into_iter()
                        .map(|r| Row {
                            values: indices
                                .iter()
                                .map(|i| {
                                    i.and_then(|idx| r.get(idx)).cloned().unwrap_or(Value::Null)
                                })
                                .collect(),
                        })
                        .collect();
                    Ok(QueryOutput {
                        tag: format!("UPDATE {updated}"),
                        columns: returning.clone(),
                        types: Self::returning_types(&tbl.columns, &returning),
                        rows,
                    })
                }
            }
            LogicalPlan::Delete {
                table_name,
                filter,
                returning,
            } => {
                let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                self.authorize(ctx, Action::Delete, ResourceRef::Table(tbl.id))?;

                let mut deleted = 0;
                let mut returning_rows = Vec::new();
                for row in self.scan_rows(tbl.id, &ctx.session_id)? {
                    if !self.row_matches(ctx, &row, &tbl.columns, filter.as_ref()) {
                        continue;
                    }
                    let pk_str = row.first().map(render).unwrap_or_default();
                    let key = format!("{}:{}", tbl.id, pk_str);
                    self.delete_row(&ctx.session_id, key)?;

                    // Maintain secondary indexes.
                    for idx in &tbl.indexes {
                        for kcol in &idx.key_columns {
                            if let Some(pos) =
                                tbl.columns.iter().position(|c| c.id == kcol.column_id)
                            {
                                let index_val = row.get(pos).unwrap_or(&Value::Null);
                                self.delete_index_entry(
                                    &ctx.session_id,
                                    idx.id,
                                    index_val,
                                    &pk_str,
                                )?;
                            }
                        }
                    }

                    deleted += 1;
                    if !returning.is_empty() {
                        returning_rows.push(row);
                    }
                }
                if returning.is_empty() {
                    Ok(QueryOutput::tag(&format!("DELETE {deleted}")))
                } else {
                    let col_names: Vec<&str> =
                        tbl.columns.iter().map(|c| c.name.as_str()).collect();
                    let indices: Vec<Option<usize>> = returning
                        .iter()
                        .map(|c| {
                            col_names
                                .iter()
                                .position(|&tc| tc == c || tc.ends_with(&format!(".{}", c)))
                        })
                        .collect();
                    let rows = returning_rows
                        .into_iter()
                        .map(|r| Row {
                            values: indices
                                .iter()
                                .map(|i| {
                                    i.and_then(|idx| r.get(idx)).cloned().unwrap_or(Value::Null)
                                })
                                .collect(),
                        })
                        .collect();
                    Ok(QueryOutput {
                        tag: format!("DELETE {deleted}"),
                        columns: returning.clone(),
                        types: Self::returning_types(&tbl.columns, &returning),
                        rows,
                    })
                }
            }
            LogicalPlan::AlterTable {
                table_name,
                operation,
            } => {
                let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;

                let change = match operation {
                    AlterTableOp::AddColumn {
                        name,
                        data_type,
                        nullable,
                    } => {
                        let column = ColumnDescriptor {
                            id: nodus_catalog::ColumnId::new(),
                            name,
                            version: 1,
                            created_at: Utc::now(),
                            updated_at: Utc::now(),
                            state: DescriptorState::Public,
                            data_type,
                            nullable,
                        };

                        // Migrate existing data to include the new column (as NULL)
                        for mut row in self.scan_rows(tbl.id, &ctx.session_id)? {
                            let pk_str = row.first().map(render).unwrap_or_default();
                            let key = format!("{}:{}", tbl.id, pk_str);
                            row.push(Value::Null); // Append null for the new column
                            self.write_row(&ctx.session_id, key, serde_json::to_string(&row)?)?;
                        }

                        nodus_catalog::TableDescriptorChange::AddColumn {
                            table_id: tbl.id,
                            column,
                        }
                    }
                    AlterTableOp::DropColumn { name } => {
                        if let Some(col_idx) = tbl.columns.iter().position(|c| c.name == name) {
                            // Cannot drop primary key (assuming first column is PK for now)
                            if col_idx == 0 {
                                anyhow::bail!("Cannot drop primary key column");
                            }

                            // Migrate existing data to remove the column
                            for mut row in self.scan_rows(tbl.id, &ctx.session_id)? {
                                let pk_str = row.first().map(render).unwrap_or_default();
                                let key = format!("{}:{}", tbl.id, pk_str);
                                if col_idx < row.len() {
                                    row.remove(col_idx);
                                }
                                self.write_row(&ctx.session_id, key, serde_json::to_string(&row)?)?;
                            }
                        } else {
                            anyhow::bail!("Column {} not found", name);
                        }

                        nodus_catalog::TableDescriptorChange::DropColumn {
                            table_id: tbl.id,
                            column_name: name,
                        }
                    }
                    AlterTableOp::RenameColumn { old_name, new_name } => {
                        nodus_catalog::TableDescriptorChange::RenameColumn {
                            table_id: tbl.id,
                            old_name,
                            new_name,
                        }
                    }
                    AlterTableOp::RenameTable { new_name } => {
                        nodus_catalog::TableDescriptorChange::RenameTable {
                            table_id: tbl.id,
                            new_name,
                        }
                    }
                };
                self.catalog_writer.update_table_descriptor(change)?;
                Ok(QueryOutput::tag("ALTER TABLE"))
            }
            LogicalPlan::CreateIndex {
                name,
                table_name,
                columns,
                unique,
            } => {
                let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;

                let mut index_cols = Vec::new();
                for c in &columns {
                    if let Some(col) = tbl.columns.iter().find(|tc| tc.name == *c) {
                        index_cols.push(nodus_catalog::IndexColumn {
                            column_id: col.id,
                            descending: false,
                        });
                    } else {
                        anyhow::bail!("Column not found for index: {}", c);
                    }
                }

                let idx_type = if unique {
                    nodus_catalog::IndexType::Unique
                } else {
                    nodus_catalog::IndexType::LocalSecondary
                };

                let index = nodus_catalog::IndexDescriptor {
                    id: nodus_catalog::IndexId::new(),
                    name,
                    version: 1,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    state: DescriptorState::Public,
                    index_type: idx_type,
                    index_state: nodus_catalog::IndexState::Creating,
                    key_columns: index_cols,
                    include_columns: vec![],
                    unique,
                    global: false,
                    predicate: None,
                    expressions: vec![],
                };

                let change = nodus_catalog::TableDescriptorChange::AddIndex {
                    table_id: tbl.id,
                    index: index.clone(),
                };
                self.catalog_writer.update_table_descriptor(change)?;

                // Backfill existing rows into the new index
                let mut seen_values = std::collections::HashSet::new();
                for row in self.scan_rows(tbl.id, &ctx.session_id)? {
                    let pk_str = row.first().map(render).unwrap_or_default();
                    for kcol in &index.key_columns {
                        if let Some(pos) = tbl.columns.iter().position(|c| c.id == kcol.column_id) {
                            let index_val = row.get(pos).unwrap_or(&Value::Null);

                            if unique {
                                let val_str = render(index_val);
                                if val_str != "NULL" && !seen_values.insert(val_str) {
                                    // Set state to Failed/Dropping or just error out. We'd need to drop it, but we can just error for now.
                                    let _ = self.catalog_writer.update_index_state(
                                        tbl.id,
                                        index.id,
                                        nodus_catalog::IndexState::Dropping,
                                    );
                                    anyhow::bail!(
                                        "Unique constraint violation during index backfill for value: {:?}",
                                        index_val
                                    );
                                }
                            }

                            self.write_index_entry(&ctx.session_id, index.id, index_val, &pk_str)?;
                        }
                    }
                }

                self.catalog_writer.update_index_state(
                    tbl.id,
                    index.id,
                    nodus_catalog::IndexState::Ready,
                )?;
                Ok(QueryOutput::tag("CREATE INDEX"))
            }
            LogicalPlan::CreateRole { name } => {
                // Must be superuser or have create role privilege in a real system
                self.catalog_writer
                    .create_role(nodus_catalog::CreateRoleRequest {
                        id: nodus_catalog::PrincipalId::new(),
                        name: name.clone(),
                        principal_type: nodus_catalog::PrincipalType::Role,
                        database_id: None,
                    })?;
                Ok(QueryOutput::tag("CREATE ROLE"))
            }
            LogicalPlan::Grant {
                privilege,
                object_name,
                grantee,
            } => {
                let role = self.catalog_reader.get_principal_by_name(&grantee)?;
                let (db_name, schema_name, table_only) = parse_object_name(&object_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                // Typically you need to own the table or have grant option
                self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
                self.catalog_writer
                    .grant_privileges(nodus_catalog::GrantPrivilegesRequest {
                        id: nodus_catalog::GrantId::new(),
                        principal_id: role.id,
                        resource: ResourceRef::Table(tbl.id),
                        privilege: privilege.clone(),
                    })?;
                Ok(QueryOutput::tag("GRANT"))
            }
            LogicalPlan::Revoke {
                privilege,
                object_name,
                revokee,
            } => {
                let role = self.catalog_reader.get_principal_by_name(&revokee)?;
                let (db_name, schema_name, table_only) = parse_object_name(&object_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
                self.catalog_writer
                    .revoke_privileges(nodus_catalog::RevokePrivilegesRequest {
                        principal_id: role.id,
                        resource: ResourceRef::Table(tbl.id),
                        privilege: privilege.clone(),
                    })?;
                Ok(QueryOutput::tag("REVOKE"))
            }
            LogicalPlan::Begin => self.exec_begin(ctx),
            LogicalPlan::Commit => self.exec_commit(ctx),
            LogicalPlan::Rollback => self.exec_rollback(ctx),
            LogicalPlan::Savepoint { name } => self.exec_savepoint(ctx, name),
            LogicalPlan::RollbackToSavepoint { name } => self.exec_rollback_to_savepoint(ctx, name),
            LogicalPlan::ReleaseSavepoint { name } => self.exec_release_savepoint(ctx, name),
            LogicalPlan::ShowVariable { variable } => self.exec_show_variable(variable),
            LogicalPlan::SetVariable {
                variable: _,
                value: _,
            } => self.exec_set_variable(),
            LogicalPlan::Noop { tag } => Ok(QueryOutput::tag(&tag)),
            LogicalPlan::SelectLiteral { values } => self.exec_select_literal(values),
            LogicalPlan::SetOp {
                op,
                all,
                left,
                right,
            } => self.exec_set_op(ctx, op, all, left, right),
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
