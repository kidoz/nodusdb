//! `ALTER TABLE`: columns added, dropped, renamed, and retyped, their
//! defaults and NOT NULL, and the table's constraints added, dropped,
//! renamed, and validated.

use crate::error_fields::DbError;
use crate::*;
use anyhow::Result;
use nodus_authz::Action;
use nodus_catalog::{
    ColumnDescriptor, DescriptorState, IndexType, ResourceRef, TableConstraint, TableDescriptor,
    TableDescriptorChange,
};

impl MemExecutor {
    /// `ALTER TABLE [IF EXISTS] table_name op, ...`: each operation sees the
    /// table as the ones before it left it.
    pub(crate) fn exec_alter_table(
        &self,
        ctx: &ExecutionContext,
        table_name: String,
        operations: Vec<AlterTableOp>,
        if_exists: bool,
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
        let tbl = match self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)
        {
            Ok(tbl) => tbl,
            Err(_) if if_exists => {
                self.notice(
                    ctx,
                    DbError::new(format!(
                        "relation \"{table_only}\" does not exist, skipping"
                    )),
                );
                return Ok(QueryOutput::tag("ALTER TABLE"));
            }
            Err(_) => anyhow::bail!("relation \"{table_only}\" does not exist"),
        };
        self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
        for operation in operations {
            let tbl = self.catalog_reader.get_table_by_id(tbl.id)?;
            self.alter_table_op(ctx, &tbl, operation)?;
        }
        Ok(QueryOutput::tag("ALTER TABLE"))
    }

    fn alter_table_op(
        &self,
        ctx: &ExecutionContext,
        tbl: &TableDescriptor,
        operation: AlterTableOp,
    ) -> Result<()> {
        match operation {
            AlterTableOp::AddColumn {
                name,
                data_type,
                nullable,
                default,
                if_not_exists,
            } => self.add_column(
                ctx,
                tbl,
                name,
                data_type,
                (nullable, default),
                if_not_exists,
            ),
            AlterTableOp::DropColumn {
                name,
                if_exists,
                cascade,
            } => self.drop_column(ctx, tbl, &name, if_exists, cascade),
            AlterTableOp::RenameColumn { old_name, new_name } => {
                self.rename_column(tbl, &old_name, &new_name)
            }
            AlterTableOp::AlterColumnType { name, data_type } => {
                column_of(tbl, &name)?;
                // Catalog-only retype: existing rows keep their stored values
                // and the type system coerces them on later reads and writes.
                self.change(TableDescriptorChange::AlterColumnType {
                    table_id: tbl.id,
                    column_name: name,
                    data_type,
                })
            }
            AlterTableOp::RenameTable { new_name } => self.rename_table(tbl, new_name),
            AlterTableOp::SetDefault { column, default } => {
                let mut column = column_of(tbl, &column)?.clone();
                column.default_expr = default.as_ref().and_then(|e| serde_json::to_string(e).ok());
                self.replace_column(tbl, column)
            }
            AlterTableOp::SetNotNull { column, not_null } => {
                self.set_not_null(ctx, tbl, &column, not_null)
            }
            AlterTableOp::AddConstraint {
                constraint,
                not_valid,
            } => self.add_constraint(ctx, tbl, constraint, not_valid),
            AlterTableOp::DropConstraint {
                name,
                if_exists,
                cascade,
            } => self.drop_constraint(ctx, tbl, &name, if_exists, cascade),
            AlterTableOp::RenameConstraint { old_name, new_name } => {
                self.rename_constraint(tbl, &old_name, &new_name)
            }
            AlterTableOp::ValidateConstraint { name } => {
                match tbl
                    .constraints
                    .iter()
                    .find(|c| c.effective_name(&tbl.name) == name)
                {
                    Some(constraint) => self.validate_constraint(ctx, tbl, constraint),
                    None => Err(constraint_missing(tbl, &name)),
                }
            }
            AlterTableOp::OwnerTo => Ok(()),
        }
    }

    fn change(&self, change: TableDescriptorChange) -> Result<()> {
        self.catalog_writer.update_table_descriptor(change)?;
        Ok(())
    }

    fn replace_column(&self, tbl: &TableDescriptor, column: ColumnDescriptor) -> Result<()> {
        self.change(TableDescriptorChange::ReplaceColumn {
            table_id: tbl.id,
            column,
        })
    }

    /// `ADD COLUMN`: existing rows get the column's default, or NULL, which
    /// a NOT NULL column refuses.
    fn add_column(
        &self,
        ctx: &ExecutionContext,
        tbl: &TableDescriptor,
        name: String,
        data_type: String,
        (nullable, default): (bool, Option<ScalarExpr>),
        if_not_exists: bool,
    ) -> Result<()> {
        if tbl.columns.iter().any(|c| c.name == name) {
            let exists = format!(
                "column \"{name}\" of relation \"{}\" already exists",
                tbl.name
            );
            if if_not_exists {
                self.notice(
                    ctx,
                    DbError::new(format!("{exists}, skipping")).code("42701"),
                );
                return Ok(());
            }
            anyhow::bail!(exists);
        }
        // Backfill value for existing rows: the evaluated DEFAULT, or NULL
        // when none is declared (PostgreSQL semantics).
        let backfill = default
            .as_ref()
            .map(|e| {
                crate::value::coerce_for_column(
                    &crate::planner::eval_scalar_expr(e, &[], &[]),
                    &data_type,
                )
            })
            .unwrap_or(Value::Null);
        let rows = self.scan_rows_keyed(tbl.id, &ctx.session_id)?;
        if !nullable && backfill == Value::Null && !rows.is_empty() {
            return Err(self.contains_nulls(tbl, &name));
        }
        let column = ColumnDescriptor {
            id: nodus_catalog::ColumnId::new(),
            name,
            version: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            state: DescriptorState::Public,
            data_type,
            nullable,
            default_expr: default.as_ref().and_then(|e| serde_json::to_string(e).ok()),
            comment: None,
        };
        // Existing rows gain the column under their stored keys.
        for (key, mut row) in rows {
            row.push(backfill.clone());
            self.write_row(&ctx.session_id, key, crate::value::encode_row(&row)?)?;
        }
        self.change(TableDescriptorChange::AddColumn {
            table_id: tbl.id,
            column,
        })
    }

    /// The error for making column `column` of `tbl` NOT NULL while a row
    /// has NULL in it.
    fn contains_nulls(&self, tbl: &TableDescriptor, column: &str) -> anyhow::Error {
        DbError::new(format!(
            "column \"{column}\" of relation \"{}\" contains null values",
            tbl.name
        ))
        .schema(self.schema_name_of(tbl))
        .table(&tbl.name)
        .column(column)
        .into()
    }

    fn set_not_null(
        &self,
        ctx: &ExecutionContext,
        tbl: &TableDescriptor,
        column: &str,
        not_null: bool,
    ) -> Result<()> {
        let position = column_position_of(tbl, column)?;
        let mut descriptor = tbl.columns[position].clone();
        if not_null {
            let has_null = self
                .scan_rows(tbl.id, &ctx.session_id)?
                .iter()
                .any(|row| row.get(position).is_none_or(|v| *v == Value::Null));
            if has_null {
                return Err(self.contains_nulls(tbl, column));
            }
        } else if Self::pk_positions_declared(tbl).contains(&position) {
            anyhow::bail!("column \"{column}\" is in a primary key");
        }
        descriptor.nullable = !not_null;
        self.replace_column(tbl, descriptor)
    }

    /// `DROP COLUMN`: the table's own constraints and indexes on the column
    /// go with it; the foreign keys of other tables referencing it and the
    /// views reading it only with `cascade`.
    fn drop_column(
        &self,
        ctx: &ExecutionContext,
        tbl: &TableDescriptor,
        name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<()> {
        let Some(position) = tbl.columns.iter().position(|c| c.name == name) else {
            let missing = format!(
                "column \"{name}\" of relation \"{}\" does not exist",
                tbl.name
            );
            if if_exists {
                self.notice(ctx, DbError::new(format!("{missing}, skipping")));
                return Ok(());
            }
            anyhow::bail!(missing);
        };
        let dependency = format!("column {name} of table {}", tbl.name);
        let mut dependents: Vec<(String, Dependent)> = Vec::new();
        for reference in self.references_to(tbl)? {
            if reference.child.id != tbl.id && reference.parent_positions.contains(&position) {
                dependents.push((
                    format!(
                        "constraint {} on table {}",
                        reference.name, reference.child.name
                    ),
                    Dependent::Constraint(reference.child.id, reference.name.clone()),
                ));
            }
        }
        for view in self.views_reading(tbl)? {
            let query = view
                .view_query
                .as_ref()
                .or(view.materialized_query.as_ref());
            if query.is_some_and(|q| plan_mentions(q, name)) {
                let kind = if view.view_query.is_some() {
                    "view"
                } else {
                    "materialized view"
                };
                dependents.push((format!("{kind} {}", view.name), Dependent::View(view)));
            }
        }
        self.settle_dependents(
            ctx,
            &dependents,
            cascade,
            &format!("cannot drop {dependency} because other objects depend on it"),
            &dependency,
        )?;

        // The table's own constraints on the column.
        for constraint in &tbl.constraints {
            let involved = match constraint {
                TableConstraint::Check { expr, .. } => sql_mentions(expr, name),
                TableConstraint::ForeignKey {
                    columns,
                    foreign_table,
                    referred_columns,
                    ..
                } => {
                    columns.iter().any(|c| c == name)
                        || (names_table(foreign_table, &tbl.name)
                            && referred_columns.iter().any(|c| c == name))
                }
            };
            if involved {
                self.change(TableDescriptorChange::DropConstraint {
                    table_id: tbl.id,
                    name: constraint.effective_name(&tbl.name),
                })?;
            }
        }
        // Its indexes on the column, the primary key's among them.
        let column_id = tbl.columns[position].id;
        let mut dropped_primary = false;
        let mut dropped: Vec<&str> = Vec::new();
        for index in &tbl.indexes {
            let on_column = index.key_columns.iter().any(|k| k.column_id == column_id)
                || index
                    .predicate
                    .as_ref()
                    .is_some_and(|p| sql_mentions(&p.sql, name));
            if on_column && !dropped.contains(&index.name.as_str()) {
                dropped.push(&index.name);
                dropped_primary |= index.index_type == IndexType::Primary;
            }
        }
        for index_name in &dropped {
            self.drop_index_named(ctx, tbl, index_name)?;
        }
        // Existing rows lose the column's value, under their stored keys.
        for (key, mut row) in self.scan_rows_keyed(tbl.id, &ctx.session_id)? {
            if position < row.len() {
                row.remove(position);
            }
            self.write_row(&ctx.session_id, key, crate::value::encode_row(&row)?)?;
        }
        self.change(TableDescriptorChange::DropColumn {
            table_id: tbl.id,
            column_name: name.to_string(),
        })?;
        if dropped_primary {
            let after = self.catalog_reader.get_table_by_id(tbl.id)?;
            self.rekey_rows(ctx, &after, false)?;
        }
        Ok(())
    }

    /// The views that read `tbl`.
    fn views_reading(&self, tbl: &TableDescriptor) -> Result<Vec<TableDescriptor>> {
        let mut views: Vec<TableDescriptor> = self
            .catalog_reader
            .list_all_tables("default")?
            .into_iter()
            .filter(|v| {
                v.id != tbl.id
                    && v.view_query
                        .as_ref()
                        .or(v.materialized_query.as_ref())
                        .is_some_and(|q| crate::ddl::plan_reads(q, &tbl.name))
            })
            .collect();
        views.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.name.cmp(&b.name)));
        Ok(views)
    }

    /// Refuses a change `dependents` depend on, unless `cascade`, which
    /// drops them (with the notice PostgreSQL gives).
    fn settle_dependents(
        &self,
        ctx: &ExecutionContext,
        dependents: &[(String, Dependent)],
        cascade: bool,
        refusal: &str,
        dependency: &str,
    ) -> Result<()> {
        if dependents.is_empty() {
            return Ok(());
        }
        if !cascade {
            let detail: Vec<String> = dependents
                .iter()
                .map(|(object, _)| format!("{object} depends on {dependency}"))
                .collect();
            return Err(DbError::new(refusal)
                .detail(detail.join("\n"))
                .hint("Use DROP ... CASCADE to drop the dependent objects too.")
                .into());
        }
        match dependents {
            [(object, _)] => self.notice(ctx, DbError::new(format!("drop cascades to {object}"))),
            many => self.notice(
                ctx,
                DbError::new(format!("drop cascades to {} other objects", many.len())).detail(
                    many.iter()
                        .map(|(object, _)| format!("drop cascades to {object}"))
                        .collect::<Vec<_>>()
                        .join("\n"),
                ),
            ),
        }
        for (_, dependent) in dependents {
            match dependent {
                Dependent::Constraint(table, name) => {
                    self.change(TableDescriptorChange::DropConstraint {
                        table_id: *table,
                        name: name.clone(),
                    })?;
                }
                Dependent::View(view) => {
                    self.catalog_writer.drop_table(view.id)?;
                }
            }
        }
        Ok(())
    }

    /// Removes the indexes of `tbl` named `name` and their entries.
    fn drop_index_named(
        &self,
        ctx: &ExecutionContext,
        tbl: &TableDescriptor,
        name: &str,
    ) -> Result<()> {
        let prefix = format!("{}:", tbl.id);
        let rows = self.scan_rows_keyed(tbl.id, &ctx.session_id)?;
        for index in tbl.indexes.iter().filter(|i| i.name == name) {
            for (key, row) in &rows {
                let pk = key.strip_prefix(&prefix).unwrap_or(key);
                for k in &index.key_columns {
                    if let Some(p) = tbl.columns.iter().position(|c| c.id == k.column_id) {
                        let value = row.get(p).unwrap_or(&Value::Null);
                        self.delete_index_entry(&ctx.session_id, index.id, value, pk)?;
                    }
                }
            }
        }
        self.change(TableDescriptorChange::DropIndex {
            table_id: tbl.id,
            index_name: name.to_string(),
        })
    }

    /// Moves each row of `tbl` (as it now is) to the key its primary key
    /// gives it, or a synthetic rowid when it has none, with its index
    /// entries. With `was_synthetic`, rows already under synthetic rowids
    /// keep them.
    fn rekey_rows(
        &self,
        ctx: &ExecutionContext,
        tbl: &TableDescriptor,
        was_synthetic: bool,
    ) -> Result<()> {
        let prefix = format!("{}:", tbl.id);
        let synthetic = Self::uses_synthetic_rowid(tbl);
        let positions = Self::pk_positions(tbl);
        for (key, row) in self.scan_rows_keyed(tbl.id, &ctx.session_id)? {
            let old_pk = key.strip_prefix(&prefix).unwrap_or(&key).to_string();
            let new_pk = match (synthetic, was_synthetic) {
                (true, true) => continue,
                (true, false) => crate::dml::synthetic_rowid(),
                (false, _) => Self::row_pk(&positions, &row),
            };
            if new_pk == old_pk {
                continue;
            }
            self.delete_row(&ctx.session_id, key.clone())?;
            self.write_row(
                &ctx.session_id,
                format!("{prefix}{new_pk}"),
                crate::value::encode_row(&row)?,
            )?;
            for index in &tbl.indexes {
                if let Some(p) = Self::index_leading_position(tbl, index) {
                    let value = row.get(p).unwrap_or(&Value::Null);
                    self.delete_index_entry(&ctx.session_id, index.id, value, &old_pk)?;
                    self.write_index_entry(&ctx.session_id, index.id, value, &new_pk)?;
                }
            }
        }
        Ok(())
    }

    /// `RENAME COLUMN`: the constraints naming the column, the table's own
    /// and other tables' foreign keys, follow it.
    fn rename_column(&self, tbl: &TableDescriptor, old: &str, new: &str) -> Result<()> {
        if !tbl.columns.iter().any(|c| c.name == old) {
            anyhow::bail!("column \"{old}\" does not exist");
        }
        if tbl.columns.iter().any(|c| c.name == new) {
            anyhow::bail!(
                "column \"{new}\" of relation \"{}\" already exists",
                tbl.name
            );
        }
        self.change(TableDescriptorChange::RenameColumn {
            table_id: tbl.id,
            old_name: old.to_string(),
            new_name: new.to_string(),
        })?;
        let rename = |names: &[String]| -> Vec<String> {
            names
                .iter()
                .map(|n| if n == old { new.to_string() } else { n.clone() })
                .collect()
        };
        // The table's own constraints.
        for constraint in &tbl.constraints {
            let renamed = match constraint {
                TableConstraint::Check { name, expr } if sql_mentions(expr, old) => {
                    TableConstraint::Check {
                        name: name.clone(),
                        expr: rename_in_sql(expr, old, new),
                    }
                }
                TableConstraint::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                } if columns.iter().any(|c| c == old)
                    || (names_table(foreign_table, &tbl.name)
                        && referred_columns.iter().any(|c| c == old)) =>
                {
                    let self_reference = names_table(foreign_table, &tbl.name);
                    TableConstraint::ForeignKey {
                        name: name.clone(),
                        columns: rename(columns),
                        foreign_table: foreign_table.clone(),
                        referred_columns: if self_reference {
                            rename(referred_columns)
                        } else {
                            referred_columns.clone()
                        },
                        on_delete: *on_delete,
                        on_update: *on_update,
                    }
                }
                _ => continue,
            };
            self.replace_constraint(tbl, constraint, renamed)?;
        }
        // Other tables' foreign keys referencing the column.
        for child in self.catalog_reader.list_all_tables("default")? {
            if child.id == tbl.id {
                continue;
            }
            for constraint in &child.constraints {
                if let TableConstraint::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                } = constraint
                    && names_table(foreign_table, &tbl.name)
                    && referred_columns.iter().any(|c| c == old)
                {
                    let renamed = TableConstraint::ForeignKey {
                        name: name.clone(),
                        columns: columns.clone(),
                        foreign_table: foreign_table.clone(),
                        referred_columns: rename(referred_columns),
                        on_delete: *on_delete,
                        on_update: *on_update,
                    };
                    self.replace_constraint(&child, constraint, renamed)?;
                }
            }
        }
        // Partial index predicates naming the column.
        for index in &tbl.indexes {
            if let Some(predicate) = &index.predicate
                && sql_mentions(&predicate.sql, old)
            {
                let mut renamed = index.clone();
                renamed.predicate = Some(nodus_catalog::Expression {
                    sql: rename_in_sql(&predicate.sql, old, new),
                });
                self.replace_index(tbl, &index.name, vec![renamed])?;
            }
        }
        Ok(())
    }

    /// Replaces `old`, a CHECK or FOREIGN KEY constraint of `tbl`, with `new`.
    fn replace_constraint(
        &self,
        tbl: &TableDescriptor,
        old: &TableConstraint,
        new: TableConstraint,
    ) -> Result<()> {
        self.change(TableDescriptorChange::DropConstraint {
            table_id: tbl.id,
            name: old.effective_name(&tbl.name),
        })?;
        self.change(TableDescriptorChange::AddConstraint {
            table_id: tbl.id,
            constraint: new,
        })
    }

    /// Replaces the indexes of `tbl` named `name` with `indexes`, which keep
    /// their ids, so their entries stay theirs.
    fn replace_index(
        &self,
        tbl: &TableDescriptor,
        name: &str,
        indexes: Vec<nodus_catalog::IndexDescriptor>,
    ) -> Result<()> {
        self.change(TableDescriptorChange::DropIndex {
            table_id: tbl.id,
            index_name: name.to_string(),
        })?;
        for index in indexes {
            self.change(TableDescriptorChange::AddIndex {
                table_id: tbl.id,
                index,
            })?;
        }
        Ok(())
    }

    /// `RENAME TO`: the foreign keys referencing the table follow it.
    fn rename_table(&self, tbl: &TableDescriptor, new_name: String) -> Result<()> {
        let (db_name, schema_name, _) = parse_object_name(&tbl.name)?;
        let new_only = new_name.rsplit('.').next().unwrap_or(&new_name).to_string();
        if self
            .catalog_reader
            .get_table(db_name, schema_name, &new_only)
            .is_ok()
        {
            anyhow::bail!("relation \"{new_only}\" already exists");
        }
        let children: Vec<TableDescriptor> = self
            .catalog_reader
            .list_all_tables("default")?
            .into_iter()
            .filter(|t| {
                t.constraints.iter().any(|c| {
                    matches!(c, TableConstraint::ForeignKey { foreign_table, .. }
                        if names_table(foreign_table, &tbl.name))
                })
            })
            .collect();
        self.change(TableDescriptorChange::RenameTable {
            table_id: tbl.id,
            new_name: new_only.clone(),
        })?;
        for child in children {
            let child = self.catalog_reader.get_table_by_id(child.id)?;
            for constraint in &child.constraints {
                if let TableConstraint::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                } = constraint
                    && names_table(foreign_table, &tbl.name)
                {
                    let renamed = TableConstraint::ForeignKey {
                        // An unnamed key keeps the name it went by.
                        name: Some(constraint.effective_name(&child.name))
                            .filter(|_| name.is_none())
                            .or_else(|| name.clone()),
                        columns: columns.clone(),
                        foreign_table: match foreign_table.rsplit_once('.') {
                            Some((schema, _)) => format!("{schema}.{new_only}"),
                            None => new_only.clone(),
                        },
                        referred_columns: referred_columns.clone(),
                        on_delete: *on_delete,
                        on_update: *on_update,
                    };
                    self.replace_constraint(&child, constraint, renamed)?;
                }
            }
        }
        Ok(())
    }

    /// The names the constraints of `tbl` go by: its CHECK and FOREIGN KEY
    /// constraints, primary key, and unique constraints.
    fn constraint_names(tbl: &TableDescriptor) -> Vec<String> {
        tbl.constraints
            .iter()
            .map(|c| c.effective_name(&tbl.name))
            .chain(
                tbl.indexes
                    .iter()
                    .filter(|i| i.unique)
                    .map(|i| i.name.clone()),
            )
            .collect()
    }

    fn add_constraint(
        &self,
        ctx: &ExecutionContext,
        tbl: &TableDescriptor,
        constraint: NewConstraint,
        not_valid: bool,
    ) -> Result<()> {
        let taken = Self::constraint_names(tbl);
        let free = |name: &str| -> Result<()> {
            if taken.iter().any(|t| t == name) {
                anyhow::bail!(
                    "constraint \"{name}\" for relation \"{}\" already exists",
                    tbl.name
                );
            }
            Ok(())
        };
        let columns: Vec<String> = tbl.columns.iter().map(|c| c.name.clone()).collect();
        match constraint {
            NewConstraint::Check { name, expr } => {
                let name = match name {
                    Some(name) => {
                        free(&name)?;
                        name
                    }
                    None => crate::ddl::check_constraint_name(&tbl.name, &columns, &expr, &taken),
                };
                let constraint = TableConstraint::Check {
                    name: Some(name),
                    expr,
                };
                if !not_valid {
                    self.validate_constraint(ctx, tbl, &constraint)?;
                }
                self.change(TableDescriptorChange::AddConstraint {
                    table_id: tbl.id,
                    constraint,
                })
            }
            NewConstraint::ForeignKey(fk) => {
                let keys = crate::referential::Keys::of(tbl);
                let fk = self.prepare_foreign_key(&keys, &self.schema_name_of(tbl), fk)?;
                free(&fk.effective_name(&tbl.name))?;
                if !not_valid {
                    self.validate_constraint(ctx, tbl, &fk)?;
                }
                self.change(TableDescriptorChange::AddConstraint {
                    table_id: tbl.id,
                    constraint: fk,
                })
            }
            NewConstraint::Unique { name, columns } => {
                let name = match name {
                    Some(name) => name,
                    None => self.unused_relation_name(&format!(
                        "{}_{}_key",
                        tbl.name,
                        columns.join("_")
                    ))?,
                };
                free(&name)?;
                if self.relation_name_taken(&name)? {
                    anyhow::bail!("relation \"{name}\" already exists");
                }
                let index = Self::new_index(tbl, name, IndexType::Unique, &columns, None)?;
                self.add_index(ctx, tbl, index)
            }
            NewConstraint::PrimaryKey { name, columns } => {
                if !Self::pk_positions_declared(tbl).is_empty() {
                    anyhow::bail!(
                        "multiple primary keys for table \"{}\" are not allowed",
                        tbl.name
                    );
                }
                let name = name.unwrap_or_else(|| format!("{}_pkey", tbl.name));
                free(&name)?;
                if self.relation_name_taken(&name)? {
                    anyhow::bail!("relation \"{name}\" already exists");
                }
                let positions = columns
                    .iter()
                    .map(|c| column_position_of(tbl, c))
                    .collect::<Result<Vec<_>>>()?;
                // A primary key's columns are NOT NULL, and its key unique.
                let rows = self.scan_rows(tbl.id, &ctx.session_id)?;
                for &p in &positions {
                    if rows
                        .iter()
                        .any(|r| r.get(p).is_none_or(|v| *v == Value::Null))
                    {
                        return Err(self.contains_nulls(tbl, &tbl.columns[p].name));
                    }
                }
                self.check_unique_key(ctx, tbl, &name, &positions, None)?;
                for &p in &positions {
                    if tbl.columns[p].nullable {
                        let mut column = tbl.columns[p].clone();
                        column.nullable = false;
                        self.replace_column(tbl, column)?;
                    }
                }
                // One primary index per column, as CREATE TABLE stores a
                // primary key.
                for column in &columns {
                    let index = Self::new_index(
                        tbl,
                        name.clone(),
                        IndexType::Primary,
                        std::slice::from_ref(column),
                        None,
                    )?;
                    self.install_index(ctx, tbl, index)?;
                }
                let after = self.catalog_reader.get_table_by_id(tbl.id)?;
                self.rekey_rows(ctx, &after, true)
            }
        }
    }

    /// Checks the rows `tbl` has against `constraint`, a CHECK constraint
    /// or foreign key of it.
    fn validate_constraint(
        &self,
        ctx: &ExecutionContext,
        tbl: &TableDescriptor,
        constraint: &TableConstraint,
    ) -> Result<()> {
        match constraint {
            TableConstraint::Check { expr, .. } => {
                let name = constraint.effective_name(&tbl.name);
                let filter = self.index_predicate(expr)?;
                let names: Vec<String> = tbl.columns.iter().map(|c| c.name.clone()).collect();
                for row in self.scan_rows(tbl.id, &ctx.session_id)? {
                    if self.eval_filter(ctx, &row, &names, &tbl.columns, Some(&filter))
                        == Some(false)
                    {
                        return Err(DbError::new(format!(
                            "check constraint \"{name}\" of relation \"{}\" is violated by some row",
                            tbl.name
                        ))
                        .schema(self.schema_name_of(tbl))
                        .table(&tbl.name)
                        .constraint(&name)
                        .into());
                    }
                }
                Ok(())
            }
            TableConstraint::ForeignKey { .. } => {
                let mut with_key = tbl.clone();
                with_key.constraints = vec![constraint.clone()];
                for row in self.scan_rows(tbl.id, &ctx.session_id)? {
                    self.check_references_from(ctx, &with_key, &row, None)?;
                }
                Ok(())
            }
        }
    }

    /// `DROP CONSTRAINT`: a CHECK or FOREIGN KEY constraint, a unique
    /// constraint or the primary key (unless foreign keys reference it,
    /// or with `cascade`), or a column's NOT NULL.
    fn drop_constraint(
        &self,
        ctx: &ExecutionContext,
        tbl: &TableDescriptor,
        name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<()> {
        if tbl
            .constraints
            .iter()
            .any(|c| c.effective_name(&tbl.name) == name)
        {
            return self.change(TableDescriptorChange::DropConstraint {
                table_id: tbl.id,
                name: name.to_string(),
            });
        }
        let key: Vec<&nodus_catalog::IndexDescriptor> = tbl
            .indexes
            .iter()
            .filter(|i| i.unique && i.name == name)
            .collect();
        if !key.is_empty() {
            let primary = key.iter().any(|i| i.index_type == IndexType::Primary);
            let mut positions: Vec<usize> = key
                .iter()
                .flat_map(|i| i.key_columns.iter())
                .filter_map(|k| tbl.columns.iter().position(|c| c.id == k.column_id))
                .collect();
            positions.sort_unstable();
            let dependents: Vec<(String, Dependent)> = self
                .references_to(tbl)?
                .into_iter()
                .filter(|r| {
                    let mut referenced = r.parent_positions.clone();
                    referenced.sort_unstable();
                    referenced == positions
                })
                .map(|r| {
                    (
                        format!("constraint {} on table {}", r.name, r.child.name),
                        Dependent::Constraint(r.child.id, r.name.clone()),
                    )
                })
                .collect();
            self.settle_dependents(
                ctx,
                &dependents,
                cascade,
                &format!(
                    "cannot drop constraint {name} on table {} because other objects depend on it",
                    tbl.name
                ),
                &format!("index {name}"),
            )?;
            self.drop_index_named(ctx, tbl, name)?;
            if primary {
                let after = self.catalog_reader.get_table_by_id(tbl.id)?;
                self.rekey_rows(ctx, &after, false)?;
            }
            return Ok(());
        }
        // A column's NOT NULL goes by `<table>_<column>_not_null`.
        let not_null = tbl
            .columns
            .iter()
            .position(|c| !c.nullable && format!("{}_{}_not_null", tbl.name, c.name) == name);
        if let Some(position) = not_null {
            return self.set_not_null(ctx, tbl, &tbl.columns[position].name, false);
        }
        if if_exists {
            self.notice(
                ctx,
                DbError::new(format!(
                    "constraint \"{name}\" of relation \"{}\" does not exist, skipping",
                    tbl.name
                )),
            );
            return Ok(());
        }
        Err(constraint_missing(tbl, name))
    }

    /// `RENAME CONSTRAINT`: a CHECK or FOREIGN KEY constraint, or a unique
    /// constraint or primary key with its index.
    fn rename_constraint(&self, tbl: &TableDescriptor, old: &str, new: &str) -> Result<()> {
        if Self::constraint_names(tbl).iter().any(|n| n == new) {
            anyhow::bail!(
                "constraint \"{new}\" for relation \"{}\" already exists",
                tbl.name
            );
        }
        if let Some(constraint) = tbl
            .constraints
            .iter()
            .find(|c| c.effective_name(&tbl.name) == old)
        {
            let renamed = match constraint.clone() {
                TableConstraint::Check { expr, .. } => TableConstraint::Check {
                    name: Some(new.to_string()),
                    expr,
                },
                TableConstraint::ForeignKey {
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    ..
                } => TableConstraint::ForeignKey {
                    name: Some(new.to_string()),
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                },
            };
            return self.replace_constraint(tbl, constraint, renamed);
        }
        let key: Vec<nodus_catalog::IndexDescriptor> = tbl
            .indexes
            .iter()
            .filter(|i| i.unique && i.name == old)
            .cloned()
            .map(|mut i| {
                i.name = new.to_string();
                i
            })
            .collect();
        if key.is_empty() {
            anyhow::bail!(
                "constraint \"{old}\" for table \"{}\" does not exist",
                tbl.name
            );
        }
        if self.relation_name_taken(new)? {
            anyhow::bail!("relation \"{new}\" already exists");
        }
        self.replace_index(tbl, old, key)
    }
}

/// An object depending on a column, constraint, or index being dropped.
enum Dependent {
    /// A foreign key: its table and name.
    Constraint(nodus_catalog::TableId, String),
    View(TableDescriptor),
}

/// The error for a constraint `tbl` does not have.
fn constraint_missing(tbl: &TableDescriptor, name: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "constraint \"{name}\" of relation \"{}\" does not exist",
        tbl.name
    )
}

fn column_position_of(tbl: &TableDescriptor, name: &str) -> Result<usize> {
    tbl.columns
        .iter()
        .position(|c| c.name == name)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "column \"{name}\" of relation \"{}\" does not exist",
                tbl.name
            )
        })
}

fn column_of<'a>(tbl: &'a TableDescriptor, name: &str) -> Result<&'a ColumnDescriptor> {
    column_position_of(tbl, name).map(|p| &tbl.columns[p])
}

/// Whether a foreign key's `foreign_table` names table `name`.
fn names_table(foreign_table: &str, name: &str) -> bool {
    foreign_table
        .rsplit('.')
        .next()
        .map(|t| t.trim_matches('"'))
        == Some(name)
}

/// The words of SQL text `sql`, as a tokenizer finds them.
fn sql_tokens(sql: &str) -> Vec<sqlparser::tokenizer::Token> {
    sqlparser::tokenizer::Tokenizer::new(&sqlparser::dialect::PostgreSqlDialect {}, sql)
        .tokenize()
        .unwrap_or_default()
}

/// Whether SQL text `sql` names `column`.
fn sql_mentions(sql: &str, column: &str) -> bool {
    sql_tokens(sql)
        .iter()
        .any(|t| matches!(t, sqlparser::tokenizer::Token::Word(w) if w.value == column))
}

/// SQL text `sql` with each name `old` spelled `new`.
fn rename_in_sql(sql: &str, old: &str, new: &str) -> String {
    use sqlparser::tokenizer::Token;
    sql_tokens(sql)
        .into_iter()
        .map(|token| match token {
            Token::Word(mut word) if word.value == old => {
                word.value = new.to_string();
                // A name that is not all lower case keeps its case quoted.
                if word.quote_style.is_none()
                    && new
                        .chars()
                        .any(|c| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'))
                {
                    word.quote_style = Some('"');
                }
                Token::Word(word).to_string()
            }
            other => other.to_string(),
        })
        .collect()
}

/// Whether the stored plan `query` of a view mentions column `column`: a
/// string in it that is the name, or ends in `.name`.
fn plan_mentions(query: &str, column: &str) -> bool {
    fn walk(value: &serde_json::Value, column: &str) -> bool {
        match value {
            serde_json::Value::String(s) => {
                s == column || s.rsplit_once('.').is_some_and(|(_, c)| c == column)
            }
            serde_json::Value::Object(map) => map.values().any(|v| walk(v, column)),
            serde_json::Value::Array(items) => items.iter().any(|v| walk(v, column)),
            _ => false,
        }
    }
    serde_json::from_str::<serde_json::Value>(query).is_ok_and(|plan| walk(&plan, column))
}
