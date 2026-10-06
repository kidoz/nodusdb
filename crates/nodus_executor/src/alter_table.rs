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
        only: bool,
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
        // An inherited table's columns pass to its descendants, unless ONLY.
        let descendants = if only {
            Vec::new()
        } else {
            self.descendants(db_name, tbl.id)?
        };
        let has_descendants = self.descendants(db_name, tbl.id)?.len() > 0;
        let ancestors = self.ancestors(db_name, tbl.id)?;
        for operation in operations {
            self.check_inherited_alter(
                &tbl,
                &descendants,
                &ancestors,
                only,
                has_descendants,
                &operation,
            )?;
            let tbl = self.catalog_reader.get_table_by_id(tbl.id)?;
            self.alter_table_op(ctx, &tbl, operation.clone(), false)?;
            for descendant in &descendants {
                let current = self.catalog_reader.get_table_by_id(descendant.id)?;
                self.alter_table_op(ctx, &current, operation.clone(), true)?;
            }
        }
        Ok(QueryOutput::tag("ALTER TABLE"))
    }

    /// The refusals PostgreSQL makes for an inherited table's `ALTER TABLE`:
    /// an `ONLY` operation that must reach children, and an operation on a
    /// column or constraint the table inherits.
    fn check_inherited_alter(
        &self,
        tbl: &TableDescriptor,
        descendants: &[TableDescriptor],
        ancestors: &[TableDescriptor],
        only: bool,
        has_descendants: bool,
        operation: &AlterTableOp,
    ) -> Result<()> {
        if only && has_descendants {
            match operation {
                AlterTableOp::AddColumn {
                    name,
                    if_not_exists,
                    ..
                } if !(*if_not_exists && tbl.columns.iter().any(|c| c.name == *name)) => {
                    anyhow::bail!("column must be added to child tables too");
                }
                AlterTableOp::RenameColumn { old_name, .. } => {
                    anyhow::bail!(
                        "inherited column \"{old_name}\" must be renamed in child tables too"
                    );
                }
                AlterTableOp::AlterColumnType { name, .. } => {
                    anyhow::bail!(
                        "type of inherited column \"{name}\" must be changed in child tables too"
                    );
                }
                AlterTableOp::AddConstraint {
                    constraint: NewConstraint::Check { .. },
                    ..
                } => {
                    anyhow::bail!("constraint must be added to child tables too");
                }
                _ => {}
            }
        }
        let _ = descendants;
        if ancestors.is_empty() {
            return Ok(());
        }
        let inherited_column = |name: &str| {
            ancestors
                .iter()
                .any(|a| a.columns.iter().any(|c| c.name == name))
        };
        let inherited_constraint = |name: &str| {
            ancestors.iter().any(|a| {
                a.constraints
                    .iter()
                    .any(|c| c.effective_name(&a.name) == name)
            })
        };
        match operation {
            AlterTableOp::RenameColumn { old_name, .. } if inherited_column(old_name) => {
                anyhow::bail!("cannot rename inherited column \"{old_name}\"");
            }
            AlterTableOp::DropColumn { name, .. } if inherited_column(name) => {
                anyhow::bail!("cannot drop inherited column \"{name}\"");
            }
            AlterTableOp::AlterColumnType { name, .. } if inherited_column(name) => {
                anyhow::bail!("cannot alter inherited column \"{name}\"");
            }
            AlterTableOp::DropConstraint { name, .. } if inherited_constraint(name) => {
                anyhow::bail!(
                    "cannot drop inherited constraint \"{name}\" of relation \"{}\"",
                    tbl.name
                );
            }
            AlterTableOp::RenameConstraint { old_name, .. } if inherited_constraint(old_name) => {
                anyhow::bail!(
                    "cannot rename inherited constraint \"{old_name}\" of relation \"{}\"",
                    tbl.name
                );
            }
            _ => {}
        }
        Ok(())
    }

    fn alter_table_op(
        &self,
        ctx: &ExecutionContext,
        tbl: &TableDescriptor,
        operation: AlterTableOp,
        propagated: bool,
    ) -> Result<()> {
        match operation {
            AlterTableOp::AddColumn {
                name,
                data_type,
                nullable,
                default,
                if_not_exists,
                sequence,
                identity,
            } => {
                // A partition's columns belong to its parent (an operation
                // the parent propagates reaches the partition as `propagated`).
                if !propagated && tbl.partition_bound.is_some() {
                    anyhow::bail!("cannot add column to a partition");
                }
                // A `serial` or identity column's sequence comes first; its
                // values fill the rows already there.
                let default = match sequence {
                    // A descendant takes the parent's sequence-backed
                    // default, never a sequence of its own.
                    Some(spec) if !propagated && !tbl.columns.iter().any(|c| c.name == name) => {
                        let schema = self
                            .catalog_reader
                            .list_schemas("default")?
                            .into_iter()
                            .find(|s| s.id == tbl.schema_id)
                            .map_or_else(|| "public".to_string(), |s| s.name);
                        let owned = crate::sequences::owned_sequence_name(&tbl.name, &name);
                        let quoted = if owned.chars().any(|c| c.is_ascii_uppercase()) {
                            format!("\"{owned}\"")
                        } else {
                            owned
                        };
                        let sequence_name = format!("{schema}.{quoted}");
                        self.create_sequence(
                            ctx,
                            &sequence_name,
                            crate::sequences::SequenceState::new(&spec)?,
                        )?;
                        let sequence_arg = ScalarExpr::Literal(Value::Text(sequence_name));
                        Some(match identity {
                            Some(always) => ScalarExpr::Function {
                                name: "__IDENTITY__".to_string(),
                                args: vec![sequence_arg, ScalarExpr::Literal(Value::Bool(always))],
                            },
                            None => ScalarExpr::Function {
                                name: crate::sequences::SERIAL.to_string(),
                                args: vec![sequence_arg],
                            },
                        })
                    }
                    _ => default,
                };
                // A descendant that already has the column (added to it
                // first) merges the definitions instead.
                if propagated && let Some(existing) = tbl.columns.iter().find(|c| c.name == name) {
                    if existing.data_type != data_type {
                        anyhow::bail!(
                            crate::error_fields::DbError::new(format!(
                                "column \"{name}\" has a type conflict"
                            ))
                            .detail(format!("{} versus {}", existing.data_type, data_type))
                            .into_text()
                        );
                    }
                    self.notice(
                        ctx,
                        DbError::new(format!(
                            "merging definition of column \"{name}\" for child \"{}\"",
                            tbl.name
                        )),
                    );
                    return Ok(());
                }
                self.add_column(
                    ctx,
                    tbl,
                    name,
                    data_type,
                    (nullable, default),
                    if_not_exists,
                )
            }
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
            AlterTableOp::RenameTable { new_name } => {
                if propagated {
                    return Ok(());
                }
                self.rename_table(tbl, new_name)
            }
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
            } => {
                // Only CHECK constraints are inherited; the rest stay on the
                // table the statement names.
                if propagated && !matches!(constraint, NewConstraint::Check { .. }) {
                    return Ok(());
                }
                self.add_constraint(ctx, tbl, constraint, not_valid)
            }
            AlterTableOp::DropConstraint {
                name,
                if_exists,
                cascade,
            } => {
                // A descendant without the constraint has nothing to drop.
                if propagated
                    && !tbl
                        .constraints
                        .iter()
                        .any(|c| c.effective_name(&tbl.name) == name)
                {
                    return Ok(());
                }
                self.drop_constraint(ctx, tbl, &name, if_exists, cascade)
            }
            AlterTableOp::RenameConstraint { old_name, new_name } => {
                if propagated
                    && !tbl
                        .constraints
                        .iter()
                        .any(|c| c.effective_name(&tbl.name) == old_name)
                {
                    return Ok(());
                }
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
            AlterTableOp::AttachPartition {
                parent,
                partition,
                bound,
            } => {
                if propagated {
                    return Ok(());
                }
                self.attach_partition(ctx, tbl, &parent, &partition, &bound)
            }
            AlterTableOp::DetachPartition { parent, partition } => {
                if propagated {
                    return Ok(());
                }
                self.detach_partition(ctx, tbl, &parent, &partition)
            }
            AlterTableOp::AlterConstraint {
                name,
                deferrable,
                initially_deferred,
            } => {
                if propagated {
                    return Ok(());
                }
                self.alter_constraint(tbl, &name, deferrable, initially_deferred)
            }
            AlterTableOp::Inherit { parent, attach } => {
                // A link belongs to the table the statement names.
                if propagated {
                    return Ok(());
                }
                let parents = self.resolve_parents(std::slice::from_ref(&parent))?;
                let parent = &parents[0];
                if attach {
                    if tbl.parents.contains(&parent.id) {
                        anyhow::bail!(
                            "relation \"{}\" would be inherited from more than once",
                            parent.name
                        );
                    }
                    self.alter_inherit_parents(tbl, std::slice::from_ref(parent))?;
                    let mut list = tbl.parents.clone();
                    list.push(parent.id);
                    self.change(TableDescriptorChange::SetParents {
                        table_id: tbl.id,
                        parents: list,
                    })
                } else {
                    if !tbl.parents.contains(&parent.id) {
                        anyhow::bail!(
                            "relation \"{}\" is not a parent of relation \"{}\"",
                            parent.name,
                            tbl.name
                        );
                    }
                    let list: Vec<_> = tbl
                        .parents
                        .iter()
                        .copied()
                        .filter(|id| *id != parent.id)
                        .collect();
                    self.change(TableDescriptorChange::SetParents {
                        table_id: tbl.id,
                        parents: list,
                    })
                }
            }
        }
    }

    /// `ALTER TABLE parent ATTACH PARTITION child FOR VALUES ...`: the child
    /// takes the parent's columns' shape, keeps every row inside the bound,
    /// and becomes a partition of the parent.
    fn attach_partition(
        &self,
        ctx: &ExecutionContext,
        parent: &TableDescriptor,
        _parent_name: &str,
        partition: &str,
        bound_text: &str,
    ) -> Result<()> {
        let Some(key_text) = parent.partition_by.clone() else {
            anyhow::bail!(
                DbError::new(format!(
                    "ALTER action ATTACH PARTITION cannot be performed on relation \"{}\"",
                    parent.name
                ))
                .detail("This operation is not supported for tables.")
                .into_text()
            );
        };
        if bound_text.trim().is_empty() {
            anyhow::bail!("partition bound specification is missing");
        }
        let (db_name, schema_name, partition_only) = parse_object_name(partition)?;
        let child = self
            .catalog_reader
            .get_table(db_name, schema_name, partition_only)?;
        if !child.parents.is_empty() {
            anyhow::bail!("\"{}\" is already a partition", child.name);
        }
        // The two are both temporary or both not.
        let parent_temp = crate::search_path::is_temp_schema(&self.schema_name_of(parent));
        let child_temp = crate::search_path::is_temp_schema(&self.schema_name_of(&child));
        if child_temp && !parent_temp {
            anyhow::bail!(
                "cannot attach a temporary relation as partition of permanent relation \"{}\"",
                parent.name
            );
        }
        if !child_temp && parent_temp {
            anyhow::bail!(
                "cannot attach a permanent relation as partition of temporary relation \"{}\"",
                parent.name
            );
        }
        // The child must have the parent's columns, in shape and NOT NULL.
        for column in &parent.columns {
            match child.columns.iter().find(|c| c.name == column.name) {
                None => anyhow::bail!("child table is missing column \"{}\"", column.name),
                Some(c) if c.data_type != column.data_type => anyhow::bail!(
                    "child table \"{}\" has different type for column \"{}\"",
                    child.name,
                    column.name
                ),
                Some(c) if !column.nullable && c.nullable => anyhow::bail!(
                    "column \"{}\" in child table \"{}\" must be marked NOT NULL",
                    column.name,
                    child.name
                ),
                Some(_) => {}
            }
        }
        if let Some(extra) = child
            .columns
            .iter()
            .find(|c| !parent.columns.iter().any(|p| p.name == c.name))
        {
            anyhow::bail!(
                DbError::new(format!(
                    "table \"{}\" contains column \"{}\" not found in parent \"{}\"",
                    child.name, extra.name, parent.name
                ))
                .detail("The new partition may contain only the columns present in parent.")
                .into_text()
            );
        }
        let key = crate::partitioning::PartitionKey::parse(&key_text)?;
        let types = crate::partitioning::key_types(parent, &key);
        let bound = crate::partitioning::parse_bound(bound_text, &key, &types)?;
        self.check_partition_bound(
            ctx,
            parent,
            &key,
            &types,
            &bound,
            &child.name,
            Some(child.id),
        )?;
        // Every row the child holds must fall inside its new bound.
        for (_key, row) in self.scan_rows_keyed(child.id, &ctx.session_id)? {
            let values = crate::partitioning::key_values(parent, &key, &row)?;
            if !crate::partitioning::contains(&bound, &values, &types) {
                anyhow::bail!(
                    "partition constraint of relation \"{}\" is violated by some row",
                    child.name
                );
            }
        }
        self.change(TableDescriptorChange::SetParents {
            table_id: child.id,
            parents: vec![parent.id],
        })?;
        self.change(TableDescriptorChange::SetPartitionBound {
            table_id: child.id,
            bound: Some(crate::partitioning::render_bound(&bound)),
        })?;
        self.create_partition_indexes(&child, parent)?;
        Ok(())
    }

    /// `ALTER TABLE t ALTER CONSTRAINT name ...`: a foreign key's
    /// `DEFERRABLE` / `INITIALLY DEFERRED` change (PostgreSQL allows no
    /// other constraint kind here).
    fn alter_constraint(
        &self,
        tbl: &TableDescriptor,
        name: &str,
        deferrable: bool,
        initially_deferred: bool,
    ) -> Result<()> {
        // A unique or primary key index of the table is no foreign key.
        if tbl.indexes.iter().any(|i| i.name == name) {
            anyhow::bail!(
                "constraint \"{name}\" of relation \"{}\" is not a foreign key constraint",
                tbl.name
            );
        }
        for constraint in &tbl.constraints {
            if constraint.effective_name(&tbl.name) != name {
                continue;
            }
            let TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            } = constraint
            else {
                anyhow::bail!(
                    "constraint \"{name}\" of relation \"{}\" is not a foreign key constraint",
                    tbl.name
                );
            };
            let updated = TableConstraint::ForeignKey {
                name: name.clone(),
                columns: columns.clone(),
                foreign_table: foreign_table.clone(),
                referred_columns: referred_columns.clone(),
                on_delete: *on_delete,
                on_update: *on_update,
                deferrable,
                initially_deferred,
            };
            return self.replace_constraint(tbl, constraint, updated);
        }
        anyhow::bail!(
            "constraint \"{name}\" of relation \"{}\" does not exist",
            tbl.name
        )
    }

    /// `ALTER TABLE parent DETACH PARTITION child`: the child keeps its rows
    /// and its indexes and stops being a partition.
    fn detach_partition(
        &self,
        ctx: &ExecutionContext,
        parent: &TableDescriptor,
        _parent_name: &str,
        partition: &str,
    ) -> Result<()> {
        if parent.partition_by.is_none() {
            anyhow::bail!(
                DbError::new(format!(
                    "ALTER action DETACH PARTITION cannot be performed on relation \"{}\"",
                    parent.name
                ))
                .detail("This operation is not supported for tables.")
                .into_text()
            );
        }
        let (db_name, schema_name, partition_only) = parse_object_name(partition)?;
        let child = self
            .catalog_reader
            .get_table(db_name, schema_name, partition_only)?;
        if !child.parents.contains(&parent.id) {
            anyhow::bail!(
                "relation \"{}\" is not a partition of relation \"{}\"",
                child.name,
                parent.name
            );
        }
        let _ = ctx;
        self.change(TableDescriptorChange::SetParents {
            table_id: child.id,
            parents: Vec::new(),
        })?;
        self.change(TableDescriptorChange::SetPartitionBound {
            table_id: child.id,
            bound: None,
        })?;
        Ok(())
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
        crate::user_types::check_type_exists(&data_type)?;
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
        // Backfill for existing rows: the DEFAULT evaluated for each (a
        // `nextval` or `random()` gives each its own), or NULL when none is
        // declared (PostgreSQL semantics).
        let backfill = || {
            default
                .as_ref()
                .map(|e| {
                    crate::value::coerce_for_column(
                        &crate::planner::eval_scalar_expr(e, &[], &[]),
                        &data_type,
                    )
                })
                .unwrap_or(Value::Null)
        };
        let rows = self.scan_rows_keyed(tbl.id, &ctx.session_id)?;
        if !nullable && default.is_none() && !rows.is_empty() {
            return Err(self.contains_nulls(tbl, &name));
        }
        let column = ColumnDescriptor {
            id: nodus_catalog::ColumnId::new(),
            name,
            version: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            state: DescriptorState::Public,
            data_type: data_type.clone(),
            nullable,
            default_expr: default.as_ref().and_then(|e| serde_json::to_string(e).ok()),
            comment: None,
        };
        // Existing rows gain the column under their stored keys.
        for (key, mut row) in rows {
            let value = backfill();
            if !nullable && value == Value::Null {
                return Err(self.contains_nulls(tbl, &column.name));
            }
            row.push(value);
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
    pub(crate) fn drop_column(
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
                if crate::index_keys::has_expressions(index)
                    && let Some(value) = Self::index_leading_value(tbl, index, row)
                {
                    self.delete_index_entry(&ctx.session_id, index.id, &value, pk)?;
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
                if let Some(value) = Self::index_leading_value(tbl, index, &row) {
                    self.delete_index_entry(&ctx.session_id, index.id, &value, &old_pk)?;
                    self.write_index_entry(&ctx.session_id, index.id, &value, &new_pk)?;
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
                    deferrable,
                    initially_deferred,
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
                        deferrable: *deferrable,
                        initially_deferred: *initially_deferred,
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
                    deferrable,
                    initially_deferred,
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
                        deferrable: *deferrable,
                        initially_deferred: *initially_deferred,
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
                    deferrable,
                    initially_deferred,
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
                        deferrable: *deferrable,
                        initially_deferred: *initially_deferred,
                    };
                    self.replace_constraint(&child, constraint, renamed)?;
                }
            }
        }
        // The views reading it follow it.
        self.retarget_renamed(&self.schema_name_of(tbl), &tbl.name, &new_only)
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
            NewConstraint::Unique {
                name,
                columns,
                deferrable,
                initially_deferred,
            } => {
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
                let mut index =
                    Self::new_index(tbl, name, IndexType::Unique, &columns, None, true)?;
                index.deferrable = deferrable;
                index.initially_deferred = initially_deferred;
                self.add_index(ctx, tbl, index)
            }
            NewConstraint::PrimaryKey {
                name,
                columns,
                deferrable,
                initially_deferred,
            } => {
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
                let key =
                    Self::new_index(tbl, name.clone(), IndexType::Unique, &columns, None, true)?;
                self.check_unique_key(ctx, tbl, &key, None)?;
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
                    let mut index = Self::new_index(
                        tbl,
                        name.clone(),
                        IndexType::Primary,
                        std::slice::from_ref(column),
                        None,
                        true,
                    )?;
                    index.deferrable = deferrable;
                    index.initially_deferred = initially_deferred;
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
                    deferrable,
                    initially_deferred,
                    ..
                } => TableConstraint::ForeignKey {
                    name: Some(new.to_string()),
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    deferrable,
                    initially_deferred,
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

impl MemExecutor {
    /// `ALTER INDEX name RENAME TO new_name`.
    pub(crate) fn exec_rename_index(
        &self,
        ctx: &ExecutionContext,
        name: &str,
        new_name: &str,
    ) -> Result<QueryOutput> {
        let (_, _, index_name) = parse_object_name(name)?;
        let tables = self.catalog_reader.list_all_tables("default")?;
        let Some(tbl) = tables
            .iter()
            .find(|t| t.indexes.iter().any(|i| i.name == index_name))
        else {
            anyhow::bail!("relation \"{index_name}\" does not exist");
        };
        self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
        if self.relation_name_taken(new_name)? {
            anyhow::bail!("relation \"{new_name}\" already exists");
        }
        let renamed: Vec<nodus_catalog::IndexDescriptor> = tbl
            .indexes
            .iter()
            .filter(|i| i.name == index_name)
            .cloned()
            .map(|mut i| {
                i.name = new_name.to_string();
                i
            })
            .collect();
        self.replace_index(tbl, index_name, renamed)?;
        Ok(QueryOutput::tag("ALTER INDEX"))
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
