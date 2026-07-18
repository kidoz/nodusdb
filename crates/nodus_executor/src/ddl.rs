//! Data-definition and RBAC statements: schema/table/view/index creation and
//! drops, ALTER TABLE, CREATE ROLE, and GRANT/REVOKE.

use crate::aggregates::*;
use crate::*;
use anyhow::Result;
use bytes::Bytes;
use chrono::Utc;
use nodus_audit::{AuditEvent, AuditSink};
use nodus_authz::{Action, AuthzContext, AuthzEngine, AuthzRequest};
use nodus_catalog::{ColumnDescriptor, CreateTableRequest, DescriptorState};

impl MemExecutor {
    pub(crate) fn exec_create_schema(
        &self,
        ctx: &ExecutionContext,
        schema_name: String,
        if_not_exists: bool,
    ) -> Result<QueryOutput> {
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
    pub(crate) fn exec_drop_schema(
        &self,
        ctx: &ExecutionContext,
        schema_name: String,
        if_exists: bool,
    ) -> Result<QueryOutput> {
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
    pub(crate) fn exec_create_table(
        &self,
        ctx: &ExecutionContext,
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<nodus_catalog::TableConstraint>,
        if_not_exists: bool,
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, table_only) = parse_object_name(&name)?;
        let db = self.catalog_reader.get_database(db_name)?;
        let sch = self.catalog_reader.get_schema(db_name, schema_name)?;
        self.authorize(ctx, Action::CreateTable, ResourceRef::Schema(sch.id))?;

        // Reject a duplicate name cleanly (creating over an existing table
        // otherwise leaves the catalog inconsistent), honoring IF NOT EXISTS.
        if self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)
            .is_ok()
        {
            if if_not_exists {
                return Ok(QueryOutput::tag("CREATE TABLE"));
            }
            anyhow::bail!("relation \"{}\" already exists", table_only);
        }
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
                // The default is stored as opaque serialized ScalarExpr and
                // evaluated by the executor at INSERT/UPDATE time.
                default_expr: c
                    .default
                    .as_ref()
                    .and_then(|e| serde_json::to_string(e).ok()),
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
    pub(crate) fn exec_create_view(
        &self,
        ctx: &ExecutionContext,
        name: String,
        query: Box<LogicalPlan>,
    ) -> Result<QueryOutput> {
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
                    default_expr: None,
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
    pub(crate) fn exec_drop_view(
        &self,
        ctx: &ExecutionContext,
        name: String,
        if_exists: bool,
    ) -> Result<QueryOutput> {
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
    pub(crate) fn exec_drop_table(
        &self,
        ctx: &ExecutionContext,
        name: String,
        if_exists: bool,
    ) -> Result<QueryOutput> {
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
    pub(crate) fn exec_alter_table(
        &self,
        ctx: &ExecutionContext,
        table_name: String,
        operation: AlterTableOp,
    ) -> Result<QueryOutput> {
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
                    default_expr: None,
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
            AlterTableOp::AlterColumnType { name, data_type } => {
                if !tbl.columns.iter().any(|c| c.name == name) {
                    anyhow::bail!("Column {} not found", name);
                }
                // Catalog-only retype: existing rows keep their stored values and
                // the type system coerces them on later reads/writes. The parsed
                // `USING <cast>` expression is not applied as a bulk rewrite.
                nodus_catalog::TableDescriptorChange::AlterColumnType {
                    table_id: tbl.id,
                    column_name: name,
                    data_type,
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
    pub(crate) fn exec_create_index(
        &self,
        ctx: &ExecutionContext,
        name: String,
        table_name: String,
        columns: Vec<String>,
        unique: bool,
        if_not_exists: bool,
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;

        if tbl.indexes.iter().any(|i| i.name == name) {
            if if_not_exists {
                return Ok(QueryOutput::tag("CREATE INDEX"));
            }
            anyhow::bail!("relation \"{}\" already exists", name);
        }

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
        let pk_positions = Self::pk_positions(&tbl);
        let mut seen_values = std::collections::HashSet::new();
        for row in self.scan_rows(tbl.id, &ctx.session_id)? {
            let pk_str = Self::row_pk(&pk_positions, &row);
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

    pub(crate) fn exec_drop_index(
        &self,
        ctx: &ExecutionContext,
        name: String,
        if_exists: bool,
    ) -> Result<QueryOutput> {
        // DROP INDEX names the index, not its table, so locate the owning table.
        let tables = self.catalog_reader.list_all_tables("default")?;
        for tbl in tables {
            if tbl.indexes.iter().any(|i| i.name == name) {
                self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
                self.catalog_writer.update_table_descriptor(
                    nodus_catalog::TableDescriptorChange::DropIndex {
                        table_id: tbl.id,
                        index_name: name.clone(),
                    },
                )?;
                return Ok(QueryOutput::tag("DROP INDEX"));
            }
        }
        if if_exists {
            Ok(QueryOutput::tag("DROP INDEX"))
        } else {
            anyhow::bail!("index \"{}\" does not exist", name)
        }
    }

    pub(crate) fn exec_create_role(
        &self,
        ctx: &ExecutionContext,
        name: String,
    ) -> Result<QueryOutput> {
        // Creating roles is a grant-management operation: require it explicitly
        // (a superuser holds ALL on System, so this still passes for them) so an
        // ordinary user can't mint roles as a privilege-escalation primitive.
        self.authorize(ctx, Action::ManageGrants, ResourceRef::System)?;
        self.catalog_writer
            .create_role(nodus_catalog::CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: name.clone(),
                principal_type: nodus_catalog::PrincipalType::Role,
                database_id: None,
            })?;
        Ok(QueryOutput::tag("CREATE ROLE"))
    }
    pub(crate) fn exec_grant(
        &self,
        ctx: &ExecutionContext,
        privilege: String,
        object_name: String,
        grantee: String,
    ) -> Result<QueryOutput> {
        let role = self.catalog_reader.get_principal_by_name(&grantee)?;
        let (db_name, schema_name, table_only) = parse_object_name(&object_name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        // Granting privileges requires grant-management authority on the object,
        // not merely CREATE on it — otherwise anyone who can create/own a table
        // could hand its privileges to any principal. (Superuser passes via
        // ALL-on-System.)
        self.authorize(ctx, Action::ManageGrants, ResourceRef::Table(tbl.id))?;
        self.catalog_writer
            .grant_privileges(nodus_catalog::GrantPrivilegesRequest {
                id: nodus_catalog::GrantId::new(),
                principal_id: role.id,
                resource: ResourceRef::Table(tbl.id),
                privilege: privilege.clone(),
            })?;
        Ok(QueryOutput::tag("GRANT"))
    }
    pub(crate) fn exec_revoke(
        &self,
        ctx: &ExecutionContext,
        privilege: String,
        object_name: String,
        revokee: String,
    ) -> Result<QueryOutput> {
        let role = self.catalog_reader.get_principal_by_name(&revokee)?;
        let (db_name, schema_name, table_only) = parse_object_name(&object_name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        self.authorize(ctx, Action::ManageGrants, ResourceRef::Table(tbl.id))?;
        self.catalog_writer
            .revoke_privileges(nodus_catalog::RevokePrivilegesRequest {
                principal_id: role.id,
                resource: ResourceRef::Table(tbl.id),
                privilege: privilege.clone(),
            })?;
        Ok(QueryOutput::tag("REVOKE"))
    }
}
