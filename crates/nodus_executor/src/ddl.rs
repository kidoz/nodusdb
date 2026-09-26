//! Data-definition and RBAC statements: schema/table/view/index creation and
//! drops, ALTER TABLE, CREATE ROLE, and GRANT/REVOKE.

use crate::aggregates::*;
use crate::error_fields::DbError;
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
        // Checked first: a replicated catalog reports a lost race only as a
        // missing schema.
        if self
            .catalog_reader
            .get_schema("default", &schema_name)
            .is_ok()
        {
            if if_not_exists {
                self.notice(
                    ctx,
                    DbError::new(format!("schema \"{schema_name}\" already exists, skipping"))
                        .code("42P06"),
                );
                return Ok(QueryOutput::tag("CREATE SCHEMA"));
            }
            anyhow::bail!("schema \"{schema_name}\" already exists");
        }
        self.catalog_writer
            .create_schema(nodus_catalog::CreateSchemaRequest {
                id: nodus_catalog::SchemaId::new(),
                database_id: db.id,
                name: schema_name,
                owner_role_id: None,
                managed_access: false,
            })?;
        Ok(QueryOutput::tag("CREATE SCHEMA"))
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
                    self.notice(
                        ctx,
                        DbError::new(format!("schema \"{schema_name}\" does not exist, skipping")),
                    );
                    Ok(QueryOutput::tag("DROP SCHEMA"))
                } else {
                    Err(anyhow::anyhow!(e))
                }
            }
        }
    }
    /// `CREATE TABLE`; with `materialized_query`, the table a materialized
    /// view stores its rows in.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn exec_create_table(
        &self,
        ctx: &ExecutionContext,
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<nodus_catalog::TableConstraint>,
        if_not_exists: bool,
        (unique_constraints, key_names): (Vec<Vec<String>>, Vec<(Vec<String>, String)>),
        materialized_query: Option<String>,
    ) -> Result<QueryOutput> {
        // The name given to the key over `columns`, if any.
        let key_name = |columns: &[String]| -> Option<String> {
            key_names.iter().find_map(|(key, name)| {
                (key.len() == columns.len() && key.iter().all(|k| columns.contains(k)))
                    .then(|| name.clone())
            })
        };
        let primary_columns: Vec<String> = columns
            .iter()
            .filter(|c| c.primary)
            .map(|c| c.name.clone())
            .collect();
        let primary_name =
            key_name(&primary_columns).unwrap_or_else(|| format!("{}_pkey", table_name_of(&name)));
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
                self.notice(ctx, Self::exists_skipping(table_only));
                return Ok(QueryOutput::tag("CREATE TABLE"));
            }
            anyhow::bail!("relation \"{}\" already exists", table_only);
        }
        let constraints = name_check_constraints(table_only, &columns, constraints);
        // Foreign keys get their names and referenced columns, checked
        // against the referenced table's keys (the new table's own, for one
        // referencing itself).
        let keys = crate::referential::Keys {
            name: table_only.to_string(),
            columns: columns.iter().map(|c| c.name.clone()).collect(),
            primary: columns
                .iter()
                .filter(|c| c.primary)
                .map(|c| c.name.clone())
                .collect(),
            unique: columns
                .iter()
                .filter(|c| c.unique && !c.primary)
                .map(|c| vec![c.name.clone()])
                .chain(unique_constraints.iter().cloned())
                .collect(),
        };
        let constraints = constraints
            .into_iter()
            .map(|c| self.prepare_foreign_key(&keys, schema_name, c))
            .collect::<Result<Vec<_>>>()?;
        // A `serial` or identity column's sequence, validated before anything
        // is created.
        let owned_sequences = columns
            .iter()
            .filter_map(|c| {
                let spec = c.sequence.as_ref()?;
                let sequence = c
                    .default
                    .as_ref()
                    .and_then(crate::sequences::default_sequence)?;
                Some(crate::sequences::SequenceState::new(spec).map(|state| (sequence, state)))
            })
            .collect::<Result<Vec<_>>>()?;
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
                comment: None,
            })
            .collect();

        let mut unique_cols = Vec::new();
        for (c, d) in columns.iter().zip(descriptors.iter()) {
            if c.unique {
                unique_cols.push((d.clone(), c.primary));
            }
        }
        // Multi-column UNIQUE constraints, resolved to column ids up front so an
        // unknown column fails before the table is created.
        let unique_groups = unique_constraints
            .iter()
            .map(|names| {
                names
                    .iter()
                    .map(|n| {
                        descriptors
                            .iter()
                            .find(|d| &d.name == n)
                            .map(|d| d.id)
                            .ok_or_else(|| {
                                anyhow::anyhow!("column \"{n}\" named in key does not exist")
                            })
                    })
                    .collect::<Result<Vec<_>>>()
                    .map(|ids| {
                        let name = key_name(names)
                            .unwrap_or_else(|| format!("{table_only}_{}_key", names.join("_")));
                        (name, ids)
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        let tbl = self.catalog_writer.create_table(CreateTableRequest {
            id: nodus_catalog::TableId::new(),
            database_id: db.id,
            schema_id: sch.id,
            name: table_only.to_string(),
            columns: descriptors,
            constraints,
            view_query: None,
            materialized_query,
        })?;

        for (col, primary) in unique_cols {
            let index = nodus_catalog::IndexDescriptor {
                id: nodus_catalog::IndexId::new(),
                name: if primary {
                    primary_name.clone()
                } else {
                    key_name(std::slice::from_ref(&col.name))
                        .unwrap_or_else(|| format!("{table_only}_{}_key", col.name))
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
        for (name, column_ids) in unique_groups {
            let index = nodus_catalog::IndexDescriptor {
                id: nodus_catalog::IndexId::new(),
                name,
                version: 1,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                state: DescriptorState::Public,
                index_type: nodus_catalog::IndexType::Unique,
                index_state: nodus_catalog::IndexState::Ready,
                key_columns: column_ids
                    .into_iter()
                    .map(|column_id| nodus_catalog::IndexColumn {
                        column_id,
                        descending: false,
                    })
                    .collect(),
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
        for (sequence, state) in owned_sequences {
            self.create_sequence(ctx, &sequence, state)?;
        }

        Ok(QueryOutput::tag("CREATE TABLE"))
    }

    /// `CREATE SEQUENCE`.
    pub(crate) fn exec_create_sequence(
        &self,
        ctx: &ExecutionContext,
        name: String,
        if_not_exists: bool,
        spec: crate::sequences::SequenceSpec,
    ) -> Result<QueryOutput> {
        let state = crate::sequences::SequenceState::new(&spec)?;
        let (db_name, schema_name, table_only) = parse_object_name(&name)?;
        if self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)
            .is_ok()
        {
            if if_not_exists {
                self.notice(ctx, Self::exists_skipping(table_only));
                return Ok(QueryOutput::tag("CREATE SEQUENCE"));
            }
            anyhow::bail!("relation \"{table_only}\" already exists");
        }
        self.create_sequence(ctx, &name, state)?;
        Ok(QueryOutput::tag("CREATE SEQUENCE"))
    }

    /// Creates a sequence relation holding `state`. Its state is committed at
    /// once, like the catalog entry, so the two never disagree.
    fn create_sequence(
        &self,
        ctx: &ExecutionContext,
        name: &str,
        state: crate::sequences::SequenceState,
    ) -> Result<()> {
        let columns = crate::sequences::SEQUENCE_COLUMNS
            .iter()
            .map(|(column, data_type)| ColumnDef {
                name: column.to_string(),
                data_type: data_type.to_string(),
                nullable: false,
                unique: false,
                primary: false,
                default: None,
                sequence: None,
            })
            .collect();
        self.exec_create_table(
            ctx,
            name.to_string(),
            columns,
            vec![],
            false,
            Default::default(),
            None,
        )?;
        let (db_name, schema_name, table_only) = parse_object_name(name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        self.sequences.initialize(&tbl, &state)
    }

    /// `DROP SEQUENCE`.
    pub(crate) fn exec_drop_sequence(
        &self,
        ctx: &ExecutionContext,
        names: Vec<String>,
        if_exists: bool,
    ) -> Result<QueryOutput> {
        for name in &names {
            let (db_name, schema_name, table_only) = parse_object_name(name)?;
            match self
                .catalog_reader
                .get_table(db_name, schema_name, table_only)
            {
                Ok(tbl) if crate::sequences::is_sequence(&tbl) => {
                    self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
                    self.catalog_writer.drop_table(tbl.id)?;
                }
                Ok(_) => anyhow::bail!("\"{table_only}\" is not a sequence"),
                Err(_) if if_exists => self.notice(
                    ctx,
                    DbError::new(format!(
                        "sequence \"{table_only}\" does not exist, skipping"
                    )),
                ),
                Err(_) => anyhow::bail!("sequence \"{table_only}\" does not exist"),
            }
        }
        Ok(QueryOutput::tag("DROP SEQUENCE"))
    }
    /// `CREATE TABLE ... AS <query>` / `SELECT ... INTO` / `CREATE
    /// MATERIALIZED VIEW`: runs the query, then creates a table with its
    /// output columns and types and, unless `WITH NO DATA`, inserts its rows.
    /// The command tag is `SELECT <n>`, as in PostgreSQL. A materialized view
    /// keeps its query for `REFRESH`.
    pub(crate) fn exec_create_table_as(
        &self,
        ctx: &ExecutionContext,
        name: String,
        query: LogicalPlan,
        if_not_exists: bool,
        (with_data, materialized): (bool, bool),
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, table_only) = parse_object_name(&name)?;
        let command = if materialized {
            "CREATE MATERIALIZED VIEW"
        } else {
            "CREATE TABLE AS"
        };
        if self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)
            .is_ok()
        {
            if if_not_exists {
                self.notice(ctx, Self::exists_skipping(table_only));
                return Ok(QueryOutput::tag(command));
            }
            anyhow::bail!("relation \"{}\" already exists", table_only);
        }
        let materialized_query = if materialized {
            Some(serde_json::to_string(&query)?)
        } else {
            None
        };
        // Without data, only the query's shape is needed.
        let query = if with_data { query } else { shape_only(query) };
        let out = self.execute_logical_inner(ctx, query)?;
        let mut columns: Vec<ColumnDef> = Vec::with_capacity(out.columns.len());
        for (col, ty) in out.columns.iter().zip(&out.types) {
            if columns.iter().any(|c| &c.name == col) {
                anyhow::bail!("column \"{col}\" specified more than once");
            }
            columns.push(ColumnDef {
                name: col.clone(),
                data_type: ty.clone(),
                nullable: true,
                unique: false,
                primary: false,
                default: None,
                sequence: None,
            });
        }
        self.exec_create_table(
            ctx,
            name.clone(),
            columns,
            vec![],
            false,
            Default::default(),
            materialized_query,
        )?;
        if !with_data {
            return Ok(QueryOutput::tag(command));
        }
        let rows: Vec<Vec<Value>> = out.rows.into_iter().map(|r| r.values).collect();
        let count = rows.len();
        if count > 0 {
            self.exec_insert(ctx, name, vec![], rows, Default::default(), None, vec![])?;
        }
        Ok(QueryOutput::tag(&format!("SELECT {count}")))
    }

    /// `REFRESH MATERIALIZED VIEW`: replaces the view's rows with its query's
    /// (or with none, `WITH NO DATA`).
    /// `COMMENT ON`: sets or removes the comment on a relation of `kind`,
    /// or on its column.
    pub(crate) fn exec_comment(
        &self,
        ctx: &ExecutionContext,
        kind: &str,
        relation: &str,
        column: Option<String>,
        comment: Option<String>,
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, table_only) = parse_object_name(relation)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)
            .map_err(|_| anyhow::anyhow!("relation \"{table_only}\" does not exist"))?;
        let sequence = crate::sequences::is_sequence(&tbl);
        let view = tbl.view_query.is_some();
        let materialized = tbl.materialized_query.is_some();
        let (fits, noun) = match kind {
            "VIEW" => (view, "a view"),
            "MATERIALIZED VIEW" => (materialized, "a materialized view"),
            "SEQUENCE" => (sequence, "a sequence"),
            "COLUMN" => (!sequence, "a table"),
            _ => (!view && !materialized && !sequence, "a table"),
        };
        if !fits {
            anyhow::bail!("\"{table_only}\" is not {noun}");
        }
        self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
        if let Some(column) = &column
            && !tbl.columns.iter().any(|c| &c.name == column)
        {
            anyhow::bail!("column \"{column}\" of relation \"{table_only}\" does not exist");
        }
        self.catalog_writer.update_table_descriptor(
            nodus_catalog::TableDescriptorChange::SetComment {
                table_id: tbl.id,
                column,
                comment: comment.filter(|c| !c.is_empty()),
            },
        )?;
        Ok(QueryOutput::tag("COMMENT"))
    }

    pub(crate) fn exec_refresh_materialized_view(
        &self,
        ctx: &ExecutionContext,
        name: String,
        with_data: bool,
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, table_only) = parse_object_name(&name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        let Some(query) = &tbl.materialized_query else {
            anyhow::bail!("\"{table_only}\" is not a materialized view");
        };
        self.authorize(ctx, Action::Insert, ResourceRef::Table(tbl.id))?;
        let rows: Vec<Vec<Value>> = if with_data {
            let plan: LogicalPlan = serde_json::from_str(query)?;
            crate::cte_scope::isolated(|| self.execute_logical_inner(ctx, plan))?
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect()
        } else {
            Vec::new()
        };
        for (key, row) in self.scan_rows_keyed(tbl.id, &ctx.session_id)? {
            self.remove_row(ctx, &tbl, &key, &row)?;
        }
        if !rows.is_empty() {
            self.exec_insert(ctx, name, vec![], rows, Default::default(), None, vec![])?;
        }
        Ok(QueryOutput::tag("REFRESH MATERIALIZED VIEW"))
    }

    /// The error for changing a materialized view's rows directly.
    pub(crate) fn reject_materialized_view(tbl: &nodus_catalog::TableDescriptor) -> Result<()> {
        if tbl.materialized_query.is_some() {
            anyhow::bail!("cannot change materialized view \"{}\"", tbl.name);
        }
        Ok(())
    }
    pub(crate) fn exec_create_view(
        &self,
        ctx: &ExecutionContext,
        name: String,
        query: Box<LogicalPlan>,
        or_replace: bool,
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, view_only) = parse_object_name(&name)?;
        let db = self.catalog_reader.get_database(db_name)?;
        let sch = self.catalog_reader.get_schema(db_name, schema_name)?;
        self.authorize(ctx, Action::CreateTable, ResourceRef::Schema(sch.id))?;
        let existing = self
            .catalog_reader
            .get_table(db_name, schema_name, view_only)
            .ok();
        match &existing {
            Some(tbl) if !or_replace => {
                anyhow::bail!("relation \"{}\" already exists", tbl.name)
            }
            Some(tbl) if tbl.view_query.is_none() => {
                anyhow::bail!("\"{}\" is not a view", tbl.name)
            }
            _ => {}
        }

        // The view's columns are its query's; stored, the query runs on
        // every read.
        let view_query_json = serde_json::to_string(&*query)?;
        let out = self.execute_logical_inner(ctx, shape_only(*query))?;
        let view_cols = out
            .columns
            .iter()
            .enumerate()
            .map(|(i, cname)| ColumnDescriptor {
                id: nodus_catalog::ColumnId::new(),
                name: cname.clone(),
                version: 1,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                state: DescriptorState::Public,
                data_type: out
                    .types
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| "VARCHAR".to_string()),
                nullable: true,
                default_expr: None,
                comment: None,
            })
            .collect();
        if let Some(tbl) = existing {
            self.catalog_writer.drop_table(tbl.id)?;
        }
        self.catalog_writer.create_table(CreateTableRequest {
            id: nodus_catalog::TableId::new(),
            database_id: db.id,
            schema_id: sch.id,
            name: view_only.to_string(),
            columns: view_cols,
            constraints: vec![],
            view_query: Some(view_query_json),
            materialized_query: None,
        })?;

        Ok(QueryOutput::tag("CREATE VIEW"))
    }
    /// `DROP TABLE`, `DROP VIEW`, or `DROP MATERIALIZED VIEW` of `names`,
    /// each only of its own kind. The foreign keys referencing a dropped
    /// relation and the views reading it depend on it: they must be dropped
    /// with it, and with `cascade` they are.
    pub(crate) fn exec_drop_relations(
        &self,
        ctx: &ExecutionContext,
        kind: RelationKind,
        names: Vec<String>,
        if_exists: bool,
        cascade: bool,
    ) -> Result<QueryOutput> {
        let mut targets: Vec<nodus_catalog::TableDescriptor> = Vec::new();
        for name in &names {
            let (db_name, schema_name, relation) = parse_object_name(name)?;
            let Ok(tbl) = self
                .catalog_reader
                .get_table(db_name, schema_name, relation)
            else {
                if if_exists {
                    self.notice(
                        ctx,
                        DbError::new(format!(
                            "{} \"{relation}\" does not exist, skipping",
                            kind.noun()
                        )),
                    );
                    continue;
                }
                anyhow::bail!("{} \"{relation}\" does not exist", kind.noun());
            };
            if RelationKind::of(&tbl) != Some(kind) {
                anyhow::bail!("\"{relation}\" is not {}", kind.article_noun());
            }
            self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
            if !targets.iter().any(|t| t.id == tbl.id) {
                targets.push(tbl);
            }
        }
        let dependents = self.dependents(&targets)?;
        if !dependents.is_empty() && !cascade {
            let message = match targets.as_slice() {
                [only] => format!(
                    "cannot drop {} {} because other objects depend on it",
                    kind.noun(),
                    only.name
                ),
                _ => {
                    "cannot drop desired object(s) because other objects depend on them".to_string()
                }
            };
            let detail: Vec<String> = dependents.iter().map(|d| d.description.clone()).collect();
            return Err(crate::error_fields::DbError::new(message)
                .detail(detail.join("\n"))
                .hint("Use DROP ... CASCADE to drop the dependent objects too.")
                .into());
        }
        match dependents.as_slice() {
            [] => {}
            [only] => self.notice(
                ctx,
                DbError::new(format!("drop cascades to {}", only.object_description())),
            ),
            many => self.notice(
                ctx,
                DbError::new(format!("drop cascades to {} other objects", many.len())).detail(
                    many.iter()
                        .map(|d| format!("drop cascades to {}", d.object_description()))
                        .collect::<Vec<_>>()
                        .join("\n"),
                ),
            ),
        }
        for dependent in &dependents {
            match &dependent.object {
                Dependent::Constraint { table, name } => {
                    self.catalog_writer.update_table_descriptor(
                        nodus_catalog::TableDescriptorChange::DropConstraint {
                            table_id: *table,
                            name: name.clone(),
                        },
                    )?;
                }
                Dependent::View(view) => self.drop_relation(view)?,
            }
        }
        for tbl in &targets {
            self.drop_relation(tbl)?;
        }
        Ok(QueryOutput::tag(kind.tag()))
    }

    /// The notice for `CREATE ... IF NOT EXISTS` of a relation that exists.
    fn exists_skipping(relation: &str) -> DbError {
        DbError::new(format!("relation \"{relation}\" already exists, skipping")).code("42P07")
    }

    /// Drops `tbl` from the catalog, with the sequence a `serial` or
    /// identity column of it owns.
    fn drop_relation(&self, tbl: &nodus_catalog::TableDescriptor) -> Result<()> {
        self.catalog_writer.drop_table(tbl.id)?;
        for column in &tbl.columns {
            let Some(sequence) = column
                .default_expr
                .as_deref()
                .and_then(|json| serde_json::from_str::<ScalarExpr>(json).ok())
                .and_then(|default| crate::sequences::default_sequence(&default))
            else {
                continue;
            };
            let owned = crate::sequences::owned_sequence_name(&tbl.name, &column.name);
            if sequence.rsplit('.').next().map(|s| s.trim_matches('"')) == Some(owned.as_str()) {
                let (db, schema, seq) = parse_object_name(&sequence)?;
                if let Ok(seq_tbl) = self.catalog_reader.get_table(db, schema, seq)
                    && crate::sequences::is_sequence(&seq_tbl)
                {
                    self.catalog_writer.drop_table(seq_tbl.id)?;
                }
            }
        }
        Ok(())
    }

    /// The objects depending on `targets` that are not among them, as
    /// PostgreSQL lists them: the foreign keys referencing each target and
    /// the views reading it, each such view followed by those reading it.
    fn dependents(
        &self,
        targets: &[nodus_catalog::TableDescriptor],
    ) -> Result<Vec<DependentObject>> {
        let mut tables = self.catalog_reader.list_all_tables("default")?;
        tables.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.name.cmp(&b.name)));
        let mut found: Vec<DependentObject> = Vec::new();
        let mut seen: Vec<nodus_catalog::TableId> = targets.iter().map(|t| t.id).collect();
        fn views_of(
            relation: &nodus_catalog::TableDescriptor,
            tables: &[nodus_catalog::TableDescriptor],
            seen: &mut Vec<nodus_catalog::TableId>,
            found: &mut Vec<DependentObject>,
        ) {
            for view in tables {
                let Some(query) = view
                    .view_query
                    .as_ref()
                    .or(view.materialized_query.as_ref())
                else {
                    continue;
                };
                if seen.contains(&view.id) || !plan_reads(query, &relation.name) {
                    continue;
                }
                seen.push(view.id);
                let kind = RelationKind::of(view).unwrap_or(RelationKind::View);
                let relation_kind = RelationKind::of(relation).unwrap_or(RelationKind::Table);
                found.push(DependentObject {
                    description: format!(
                        "{} {} depends on {} {}",
                        kind.noun(),
                        view.name,
                        relation_kind.noun(),
                        relation.name
                    ),
                    object: Dependent::View(view.clone()),
                });
                views_of(view, tables, seen, found);
            }
        }
        for target in targets {
            for reference in self.references_to(target)? {
                if targets.iter().any(|t| t.id == reference.child.id) {
                    continue;
                }
                found.push(DependentObject {
                    description: format!(
                        "constraint {} on table {} depends on {} {}",
                        reference.name,
                        reference.child.name,
                        RelationKind::of(target)
                            .unwrap_or(RelationKind::Table)
                            .noun(),
                        target.name
                    ),
                    object: Dependent::Constraint {
                        table: reference.child.id,
                        name: reference.name.clone(),
                    },
                });
            }
            views_of(target, &tables, &mut seen, &mut found);
        }
        Ok(found)
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
                default,
            } => {
                // Backfill value for existing rows: the evaluated DEFAULT, or
                // NULL when none is declared (PostgreSQL semantics).
                let backfill = default
                    .as_ref()
                    .map(|e| {
                        crate::value::coerce_for_column(
                            &crate::planner::eval_scalar_expr(e, &[], &[]),
                            &data_type,
                        )
                    })
                    .unwrap_or(Value::Null);
                let column = ColumnDescriptor {
                    id: nodus_catalog::ColumnId::new(),
                    name,
                    version: 1,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    state: DescriptorState::Public,
                    data_type,
                    nullable,
                    default_expr: default.as_ref().and_then(|e| serde_json::to_string(e).ok()),
                    comment: None,
                };

                // Migrate existing data to include the new column, addressing
                // each row by its actual stored key (works for any key scheme).
                for (key, mut row) in self.scan_rows_keyed(tbl.id, &ctx.session_id)? {
                    row.push(backfill.clone());
                    self.write_row(&ctx.session_id, key, crate::value::encode_row(&row)?)?;
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

                    // Migrate existing data to remove the column, addressing
                    // each row by its actual stored key (works for any key
                    // scheme, not just first-column PKs).
                    for (key, mut row) in self.scan_rows_keyed(tbl.id, &ctx.session_id)? {
                        if col_idx < row.len() {
                            row.remove(col_idx);
                        }
                        self.write_row(&ctx.session_id, key, crate::value::encode_row(&row)?)?;
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
                if !tbl.columns.iter().any(|c| c.name == old_name) {
                    anyhow::bail!("column \"{old_name}\" does not exist");
                }
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

    /// `CREATE [UNIQUE] INDEX [name] ON table (columns) [WHERE predicate]`.
    pub(crate) fn exec_create_index(
        &self,
        ctx: &ExecutionContext,
        name: String,
        table_name: String,
        columns: Vec<String>,
        (unique, predicate): (bool, Option<String>),
        if_not_exists: bool,
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
        let name = if name.is_empty() {
            self.unused_relation_name(&format!("{table_only}_{}_idx", columns.join("_")))?
        } else {
            name
        };
        if self.relation_name_taken(&name)? {
            if if_not_exists {
                self.notice(ctx, Self::exists_skipping(&name));
                return Ok(QueryOutput::tag("CREATE INDEX"));
            }
            anyhow::bail!("relation \"{}\" already exists", name);
        }
        let index_type = if unique {
            nodus_catalog::IndexType::Unique
        } else {
            nodus_catalog::IndexType::LocalSecondary
        };
        let index = Self::new_index(&tbl, name, index_type, &columns, predicate)?;
        self.add_index(ctx, &tbl, index)?;
        Ok(QueryOutput::tag("CREATE INDEX"))
    }

    /// An index of `tbl` named `name` over `columns`.
    pub(crate) fn new_index(
        tbl: &nodus_catalog::TableDescriptor,
        name: String,
        index_type: nodus_catalog::IndexType,
        columns: &[String],
        predicate: Option<String>,
    ) -> Result<nodus_catalog::IndexDescriptor> {
        let key_columns = columns
            .iter()
            .map(|c| {
                tbl.columns
                    .iter()
                    .find(|tc| &tc.name == c)
                    .map(|col| nodus_catalog::IndexColumn {
                        column_id: col.id,
                        descending: false,
                    })
                    .ok_or_else(|| anyhow::anyhow!("column \"{c}\" does not exist"))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(nodus_catalog::IndexDescriptor {
            id: nodus_catalog::IndexId::new(),
            name,
            version: 1,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: DescriptorState::Public,
            unique: index_type != nodus_catalog::IndexType::LocalSecondary,
            index_type,
            index_state: nodus_catalog::IndexState::Creating,
            key_columns,
            include_columns: vec![],
            global: false,
            predicate: predicate.map(|sql| nodus_catalog::Expression { sql }),
            expressions: vec![],
        })
    }

    /// Adds `index` to `tbl` and fills it from the rows `tbl` has. A unique
    /// index is refused when two rows share a key (rows its predicate leaves
    /// out, and keys with a NULL, aside).
    pub(crate) fn add_index(
        &self,
        ctx: &ExecutionContext,
        tbl: &nodus_catalog::TableDescriptor,
        index: nodus_catalog::IndexDescriptor,
    ) -> Result<()> {
        if index.unique {
            let positions: Vec<usize> = index
                .key_columns
                .iter()
                .filter_map(|k| tbl.columns.iter().position(|c| c.id == k.column_id))
                .collect();
            let predicate = index.predicate.as_ref().map(|p| p.sql.as_str());
            self.check_unique_key(ctx, tbl, &index.name, &positions, predicate)?;
        }
        self.install_index(ctx, tbl, index)
    }

    /// Rejects unique key `positions` of `tbl`, for index `name`, when two
    /// of its rows share it (rows `predicate` leaves out, and keys with a
    /// NULL, aside).
    pub(crate) fn check_unique_key(
        &self,
        ctx: &ExecutionContext,
        tbl: &nodus_catalog::TableDescriptor,
        name: &str,
        positions: &[usize],
        predicate: Option<&str>,
    ) -> Result<()> {
        let predicate = match predicate {
            Some(sql) => Some(self.index_predicate(sql)?),
            None => None,
        };
        let names: Vec<String> = tbl.columns.iter().map(|c| c.name.clone()).collect();
        let mut seen = std::collections::HashSet::new();
        for row in self.scan_rows(tbl.id, &ctx.session_id)? {
            if let Some(filter) = &predicate
                && self.eval_filter(ctx, &row, &names, &tbl.columns, Some(filter)) != Some(true)
            {
                continue;
            }
            let Some(key) = crate::constraints::key_tuple(&row, positions) else {
                continue;
            };
            let rendered: Vec<String> = key
                .iter()
                .map(|v| render(&crate::value::key_form(v)))
                .collect();
            if !seen.insert(rendered.join("\u{1}")) {
                let columns: Vec<&str> = positions
                    .iter()
                    .map(|&p| tbl.columns[p].name.as_str())
                    .collect();
                let values: Vec<String> = key.iter().map(render).collect();
                return Err(
                    DbError::new(format!("could not create unique index \"{name}\""))
                        .detail(format!(
                            "Key ({})=({}) is duplicated.",
                            columns.join(", "),
                            values.join(", ")
                        ))
                        .schema(self.schema_name_of(tbl))
                        .table(&tbl.name)
                        .constraint(name)
                        .into(),
                );
            }
        }
        Ok(())
    }

    /// Adds `index` to `tbl`'s descriptor and writes its entries for the
    /// rows `tbl` has, under their stored keys.
    pub(crate) fn install_index(
        &self,
        ctx: &ExecutionContext,
        tbl: &nodus_catalog::TableDescriptor,
        index: nodus_catalog::IndexDescriptor,
    ) -> Result<()> {
        let leading = Self::index_leading_position(tbl, &index);
        self.catalog_writer.update_table_descriptor(
            nodus_catalog::TableDescriptorChange::AddIndex {
                table_id: tbl.id,
                index: index.clone(),
            },
        )?;
        let prefix = format!("{}:", tbl.id);
        for (key, row) in self.scan_rows_keyed(tbl.id, &ctx.session_id)? {
            let pk = key.strip_prefix(&prefix).unwrap_or(&key);
            if let Some(pos) = leading {
                let value = row.get(pos).unwrap_or(&Value::Null);
                self.write_index_entry(&ctx.session_id, index.id, value, pk)?;
            }
        }
        self.catalog_writer.update_index_state(
            tbl.id,
            index.id,
            nodus_catalog::IndexState::Ready,
        )?;
        Ok(())
    }

    /// Whether a relation (table, view, sequence, or index) is named `name`.
    pub(crate) fn relation_name_taken(&self, name: &str) -> Result<bool> {
        Ok(self
            .catalog_reader
            .list_all_tables("default")?
            .iter()
            .any(|t| t.name == name || t.indexes.iter().any(|i| i.name == name)))
    }

    /// `base`, or with the first number after it that makes it no
    /// relation's name, as PostgreSQL names an index.
    pub(crate) fn unused_relation_name(&self, base: &str) -> Result<String> {
        let mut name = base.to_string();
        let mut n = 0;
        while self.relation_name_taken(&name)? {
            n += 1;
            name = format!("{base}{n}");
        }
        Ok(name)
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
            self.notice(
                ctx,
                DbError::new(format!("index \"{name}\" does not exist, skipping")),
            );
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
        if name.eq_ignore_ascii_case(nodus_catalog::PUBLIC_ROLE) {
            anyhow::bail!("role name \"{name}\" is reserved");
        }
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
        let public = grantee.eq_ignore_ascii_case(nodus_catalog::PUBLIC_ROLE);
        let role = self.catalog_reader.get_principal_by_name(if public {
            nodus_catalog::PUBLIC_ROLE
        } else {
            &grantee
        });
        let (db_name, schema_name, table_only) = parse_object_name(&object_name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        // Granting privileges requires grant-management authority on the object,
        // not merely CREATE on it — otherwise anyone who can create/own a table
        // could hand its privileges to any principal. (Superuser passes via
        // ALL-on-System.)
        self.authorize(ctx, Action::ManageGrants, ResourceRef::Table(tbl.id))?;
        // The role behind PUBLIC comes with the first grant to it.
        let role = match role {
            Err(_) if public => {
                self.catalog_writer
                    .create_role(nodus_catalog::CreateRoleRequest {
                        id: nodus_catalog::PrincipalId::new(),
                        name: nodus_catalog::PUBLIC_ROLE.to_string(),
                        principal_type: nodus_catalog::PrincipalType::Public,
                        database_id: None,
                    })?
            }
            role => role?,
        };
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
        let public = revokee.eq_ignore_ascii_case(nodus_catalog::PUBLIC_ROLE);
        let role = match self.catalog_reader.get_principal_by_name(if public {
            nodus_catalog::PUBLIC_ROLE
        } else {
            &revokee
        }) {
            // Nothing was ever granted to PUBLIC.
            Err(_) if public => None,
            role => Some(role?),
        };
        let (db_name, schema_name, table_only) = parse_object_name(&object_name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        self.authorize(ctx, Action::ManageGrants, ResourceRef::Table(tbl.id))?;
        let Some(role) = role else {
            return Ok(QueryOutput::tag("REVOKE"));
        };
        self.catalog_writer
            .revoke_privileges(nodus_catalog::RevokePrivilegesRequest {
                principal_id: role.id,
                resource: ResourceRef::Table(tbl.id),
                privilege: privilege.clone(),
            })?;
        Ok(QueryOutput::tag("REVOKE"))
    }
}

/// The kinds of relation `DROP` removes, each only by its own statement.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelationKind {
    Table,
    View,
    MaterializedView,
}

impl RelationKind {
    /// The kind of `tbl`; `None` for a sequence.
    fn of(tbl: &nodus_catalog::TableDescriptor) -> Option<RelationKind> {
        if crate::sequences::is_sequence(tbl) {
            None
        } else if tbl.view_query.is_some() {
            Some(RelationKind::View)
        } else if tbl.materialized_query.is_some() {
            Some(RelationKind::MaterializedView)
        } else {
            Some(RelationKind::Table)
        }
    }

    fn noun(self) -> &'static str {
        match self {
            RelationKind::Table => "table",
            RelationKind::View => "view",
            RelationKind::MaterializedView => "materialized view",
        }
    }

    fn article_noun(self) -> &'static str {
        match self {
            RelationKind::Table => "a table",
            RelationKind::View => "a view",
            RelationKind::MaterializedView => "a materialized view",
        }
    }

    fn tag(self) -> &'static str {
        match self {
            RelationKind::Table => "DROP TABLE",
            RelationKind::View => "DROP VIEW",
            RelationKind::MaterializedView => "DROP MATERIALIZED VIEW",
        }
    }
}

/// An object that depends on a relation being dropped.
struct DependentObject {
    /// As PostgreSQL's DETAIL lists it: `view v depends on table t`.
    description: String,
    object: Dependent,
}

impl DependentObject {
    /// The object as a notice names it: `view v`, `constraint c on table t`.
    fn object_description(&self) -> String {
        self.description
            .split(" depends on ")
            .next()
            .unwrap_or(&self.description)
            .to_string()
    }
}

enum Dependent {
    /// A foreign key of another table.
    Constraint {
        table: nodus_catalog::TableId,
        name: String,
    },
    View(nodus_catalog::TableDescriptor),
}

/// Whether the stored plan `query` of a view reads relation `name`.
pub(crate) fn plan_reads(query: &str, name: &str) -> bool {
    fn walk(value: &serde_json::Value, name: &str) -> bool {
        match value {
            serde_json::Value::Object(map) => map.iter().any(|(key, v)| {
                let reads = key == "table_name"
                    && v.as_str().is_some_and(|t| {
                        t.rsplit('.').next().map(|t| t.trim_matches('"')) == Some(name)
                    });
                reads || walk(v, name)
            }),
            serde_json::Value::Array(items) => items.iter().any(|v| walk(v, name)),
            _ => false,
        }
    }
    serde_json::from_str::<serde_json::Value>(query).is_ok_and(|plan| walk(&plan, name))
}

/// Names each unnamed CHECK constraint as PostgreSQL does:
/// `<table>_<column>_check` when it names one column, else `<table>_check`,
/// numbered when the name is taken.
fn name_check_constraints(
    table: &str,
    columns: &[ColumnDef],
    constraints: Vec<nodus_catalog::TableConstraint>,
) -> Vec<nodus_catalog::TableConstraint> {
    use nodus_catalog::TableConstraint;
    let mut taken: Vec<String> = constraints
        .iter()
        .filter_map(|c| match c {
            TableConstraint::Check { name, .. } | TableConstraint::ForeignKey { name, .. } => {
                name.clone()
            }
        })
        .collect();
    constraints
        .into_iter()
        .map(|constraint| match constraint {
            TableConstraint::Check { name: None, expr } => {
                let mut named: Vec<&str> = expr
                    .split(|c: char| !(c.is_alphanumeric() || c == '_'))
                    .filter(|word| columns.iter().any(|c| c.name == *word))
                    .collect();
                named.sort_unstable();
                named.dedup();
                let base = match named.as_slice() {
                    [column] => format!("{table}_{column}_check"),
                    _ => format!("{table}_check"),
                };
                let mut name = base.clone();
                let mut n = 0;
                while taken.contains(&name) {
                    n += 1;
                    name = format!("{base}{n}");
                }
                taken.push(name.clone());
                TableConstraint::Check {
                    name: Some(name),
                    expr,
                }
            }
            other => other,
        })
        .collect()
}

/// A relation name without its schema.
fn table_name_of(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name).trim_matches('"')
}

/// A query that yields its result's columns and types but no rows, when it
/// is a SELECT (other queries run in full).
fn shape_only(query: LogicalPlan) -> LogicalPlan {
    match query {
        LogicalPlan::Select {
            ctes,
            table_name,
            table_alias,
            joins,
            projection,
            group_by,
            filter,
            having,
            grouping_sets,
            order_by,
            offset,
            distinct,
            sort,
            group_exprs,
            distinct_on,
            ..
        } => LogicalPlan::Select {
            ctes,
            table_name,
            table_alias,
            joins,
            projection,
            group_by,
            filter,
            having,
            grouping_sets,
            order_by,
            limit: Some(0),
            offset,
            distinct,
            sort,
            group_exprs,
            distinct_on,
        },
        other => other,
    }
}
