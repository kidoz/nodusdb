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
    /// `CREATE TABLE`; with `materialized_query`, the table a materialized
    /// view stores its rows in.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn exec_create_table(
        &self,
        ctx: &ExecutionContext,
        name: String,
        mut columns: Vec<ColumnDef>,
        constraints: Vec<nodus_catalog::TableConstraint>,
        if_not_exists: bool,
        (unique_constraints, key_names, key_flags): (
            Vec<Vec<String>>,
            Vec<(Vec<String>, String)>,
            Vec<(Vec<String>, bool, bool, bool)>,
        ),
        materialized_query: Option<String>,
        inherits: Vec<String>,
        partition: (Option<String>, Option<String>, Option<String>),
    ) -> Result<QueryOutput> {
        // The name given to the key over `columns`, if any.
        let key_name = |columns: &[String]| -> Option<String> {
            key_names.iter().find_map(|(key, name)| {
                (key.len() == columns.len() && key.iter().all(|k| columns.contains(k)))
                    .then(|| name.clone())
            })
        };
        // The `(DEFERRABLE, INITIALLY DEFERRED, NULLS NOT DISTINCT)` of the
        // key over `columns`, if it was marked so.
        let key_flags = |columns: &[String]| -> (bool, bool, bool) {
            key_flags
                .iter()
                .find(|(key, _, _, _)| {
                    key.len() == columns.len() && key.iter().all(|k| columns.contains(k))
                })
                .map(|(_, deferrable, initially, nnd)| (*deferrable, *initially, *nnd))
                .unwrap_or((false, false, false))
        };
        let primary_columns: Vec<String> = columns
            .iter()
            .filter(|c| c.primary)
            .map(|c| c.name.clone())
            .collect();
        let primary_name =
            key_name(&primary_columns).unwrap_or_else(|| format!("{}_pkey", table_name_of(&name)));
        let (db_name, schema_name, table_only) = parse_object_name(&name)?;
        // A temporary relation's schema is made for the session on first use.
        let temp = crate::search_path::is_temp_schema(schema_name);
        if temp {
            self.ensure_temp_schema(ctx)?;
        }
        let db = self.catalog_reader.get_database(db_name)?;
        let sch = self.catalog_reader.get_schema(db_name, schema_name)?;
        if !temp {
            self.authorize(ctx, Action::CreateTable, ResourceRef::Schema(sch.id))?;
        }

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
        // `PARTITION BY` fixes the table's key; `PARTITION OF` makes the new
        // table a partition of the parent its bound names.
        let (partition_by, partition_of, for_values) = partition;
        let mut parents = self.resolve_parents(&inherits)?;
        let mut constraints = constraints;
        let mut partition_bound_text: Option<String> = None;
        if let Some(parent_name) = &partition_of {
            if !parents.is_empty() {
                anyhow::bail!("cannot use \"INHERITS\" with \"PARTITION OF\"");
            }
            if !columns.is_empty() {
                anyhow::bail!("cannot specify columns for a partition");
            }
            let (parent_db, parent_schema, parent_only) = parse_object_name(parent_name)?;
            let parent = self
                .catalog_reader
                .get_table(parent_db, parent_schema, parent_only)?;
            if parent.view_query.is_some() {
                anyhow::bail!(
                    "inherited relation \"{parent_only}\" is not a table or foreign table"
                );
            }
            // A partition and its parent are both temporary or both not.
            let parent_temp = crate::search_path::is_temp_schema(parent_schema);
            if parent_temp && !temp {
                anyhow::bail!(
                    "cannot create a permanent relation as partition of temporary relation \"{parent_only}\""
                );
            }
            if temp && !parent_temp {
                anyhow::bail!(
                    "cannot create a temporary relation as partition of permanent relation \"{parent_only}\""
                );
            }
            let Some(key_text) = parent.partition_by.clone() else {
                anyhow::bail!("\"{parent_only}\" is not partitioned");
            };
            let Some(bound_spec) = for_values else {
                anyhow::bail!("partition bound specification is missing");
            };
            let key = crate::partitioning::PartitionKey::parse(&key_text)?;
            let types = crate::partitioning::key_types(&parent, &key);
            let bound = crate::partitioning::parse_bound(&bound_spec, &key, &types)?;
            self.check_partition_bound(ctx, &parent, &key, &types, &bound, table_only, None)?;
            // The partition takes the parent's columns (and their defaults)
            // and CHECK constraints.
            columns = parent
                .columns
                .iter()
                .map(|c| ColumnDef {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                    unique: false,
                    primary: false,
                    default: c
                        .default_expr
                        .as_deref()
                        .and_then(|e| serde_json::from_str(e).ok()),
                    sequence: None,
                })
                .collect();
            constraints.extend(
                parent
                    .constraints
                    .iter()
                    .filter(|c| matches!(c, nodus_catalog::TableConstraint::Check { .. }))
                    .cloned(),
            );
            parents = vec![parent];
            partition_bound_text = Some(crate::partitioning::render_bound(&bound));
        } else if !parents.is_empty() {
            // The parents' columns lead the new table's, and their CHECK
            // constraints come along; a conflict fails before anything is made.
            let (merged, inherited_checks) = self.merge_inherited(ctx, &parents, columns)?;
            columns = merged;
            constraints.extend(inherited_checks);
        }
        // A key of the new table's own: its columns must exist and every
        // unique constraint must cover it.
        if let Some(key_text) = &partition_by {
            let key = crate::partitioning::PartitionKey::parse(key_text)?;
            for column in &key.columns {
                if !columns.iter().any(|c| &c.name == column) {
                    anyhow::bail!("column \"{column}\" named in partition key does not exist");
                }
            }
            let mut uniques: Vec<(&str, Vec<String>)> = Vec::new();
            let primary: Vec<String> = columns
                .iter()
                .filter(|c| c.primary)
                .map(|c| c.name.clone())
                .collect();
            if !primary.is_empty() {
                uniques.push(("PRIMARY KEY", primary));
            }
            for c in columns.iter().filter(|c| c.unique && !c.primary) {
                uniques.push(("UNIQUE", vec![c.name.clone()]));
            }
            for group in &unique_constraints {
                uniques.push(("UNIQUE", group.clone()));
            }
            for (kind, columns) in uniques {
                if let Some(missing) = key.columns.iter().find(|column| !columns.contains(column)) {
                    anyhow::bail!(DbError::new(
                        "unique constraint on partitioned table must include all partitioning columns"
                    )
                    .detail(format!(
                        "{kind} constraint on table \"{table_only}\" lacks column \"{missing}\" which is part of the partition key."
                    ))
                    .into_text());
                }
            }
        }
        self.reject_type_name(schema_name, table_only)?;
        for column in &columns {
            crate::user_types::check_type_exists(&column.data_type)?;
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
                        let flags = key_flags(names);
                        (name, ids, flags)
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
            parents: parents.iter().map(|p| p.id).collect(),
            partition_by: partition_by.clone(),
            partition_bound: partition_bound_text.clone(),
        })?;

        // A composite primary key is one index per column, but its
        // deferrability belongs to the whole key.
        let primary_flags = key_flags(&primary_columns);
        for (col, primary) in unique_cols {
            let (deferrable, initially_deferred, nulls_not_distinct) = if primary {
                primary_flags
            } else {
                key_flags(std::slice::from_ref(&col.name))
            };
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
                constraint: true,
                deferrable,
                initially_deferred,
                nulls_not_distinct,
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
        for (name, column_ids, flags) in unique_groups {
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
                constraint: true,
                deferrable: flags.0,
                initially_deferred: flags.1,
                nulls_not_distinct: flags.2,
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
        // A partition carries a copy of each of its parent's indexes.
        if partition_bound_text.is_some()
            && let Some(parent) = parents.first()
        {
            self.create_partition_indexes(&tbl, parent)?;
        }

        Ok(QueryOutput::tag("CREATE TABLE"))
    }

    /// The partition copies of a parent's indexes, named as PostgreSQL names
    /// them: `<partition>_pkey`, `<partition>_<columns>_key` for a
    /// constraint's, `<partition>_<columns>_idx` for a plain one.
    pub(crate) fn create_partition_indexes(
        &self,
        partition: &nodus_catalog::TableDescriptor,
        parent: &nodus_catalog::TableDescriptor,
    ) -> Result<()> {
        let fresh = self.catalog_reader.get_table_by_id(partition.id)?;
        let partition = &fresh;
        for index in &parent.indexes {
            // An expression key part has no column of its own.
            let names: Vec<String> = index
                .key_columns
                .iter()
                .map(|k| {
                    if k.column_id == crate::index_keys::EXPRESSION_KEY {
                        "expr".to_string()
                    } else {
                        parent
                            .columns
                            .iter()
                            .find(|c| c.id == k.column_id)
                            .map(|c| c.name.clone())
                            .unwrap_or_default()
                    }
                })
                .collect();
            // A partition that already has an index over the same columns
            // keeps it, as PostgreSQL attaches it rather than making one —
            // but a constraint's own index belongs to that constraint.
            let covered = partition.indexes.iter().any(|existing| {
                let existing_names: Vec<String> = existing
                    .key_columns
                    .iter()
                    .filter_map(|k| {
                        partition
                            .columns
                            .iter()
                            .find(|c| c.id == k.column_id)
                            .map(|c| c.name.clone())
                    })
                    .collect();
                existing_names == names
                    && existing.constraint == index.constraint
                    && existing.unique == index.unique
                    && existing.predicate.as_ref().map(|p| &p.sql)
                        == index.predicate.as_ref().map(|p| &p.sql)
            });
            if covered {
                continue;
            }
            let name = match index.index_type {
                nodus_catalog::IndexType::Primary => format!("{}_pkey", partition.name),
                nodus_catalog::IndexType::Unique if index.constraint => {
                    format!("{}_{}_key", partition.name, names.join("_"))
                }
                _ => format!("{}_{}_idx", partition.name, names.join("_")),
            };
            let key_columns = index
                .key_columns
                .iter()
                .filter_map(|k| {
                    if k.column_id == crate::index_keys::EXPRESSION_KEY {
                        return Some(nodus_catalog::IndexColumn {
                            column_id: crate::index_keys::EXPRESSION_KEY,
                            descending: k.descending,
                        });
                    }
                    parent
                        .columns
                        .iter()
                        .find(|c| c.id == k.column_id)
                        .and_then(|c| {
                            partition
                                .columns
                                .iter()
                                .find(|p| p.name == c.name)
                                .map(|p| nodus_catalog::IndexColumn {
                                    column_id: p.id,
                                    descending: k.descending,
                                })
                        })
                })
                .collect();
            let mirror = nodus_catalog::IndexDescriptor {
                id: nodus_catalog::IndexId::new(),
                name,
                version: 1,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                state: DescriptorState::Public,
                index_type: index.index_type.clone(),
                index_state: nodus_catalog::IndexState::Ready,
                key_columns,
                include_columns: vec![],
                unique: index.unique,
                constraint: index.constraint,
                deferrable: index.deferrable,
                initially_deferred: index.initially_deferred,
                nulls_not_distinct: index.nulls_not_distinct,
                global: index.global,
                predicate: index.predicate.clone(),
                expressions: index.expressions.clone(),
            };
            self.catalog_writer.update_table_descriptor(
                nodus_catalog::TableDescriptorChange::AddIndex {
                    table_id: partition.id,
                    index: mirror,
                },
            )?;
        }
        Ok(())
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
        // A temporary relation's schema is made for the session on first use.
        let temp = crate::search_path::is_temp_schema(schema_name);
        if temp {
            self.ensure_temp_schema(ctx)?;
        }
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
        self.reject_type_name(schema_name, table_only)?;
        self.create_sequence(ctx, &name, state)?;
        Ok(QueryOutput::tag("CREATE SEQUENCE"))
    }

    /// Creates a sequence relation holding `state`. Its state is committed at
    /// once, like the catalog entry, so the two never disagree.
    pub(crate) fn create_sequence(
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
            Vec::new(),
            (None, None, None),
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
    /// `ALTER SEQUENCE [IF EXISTS] name options`.
    pub(crate) fn exec_alter_sequence(
        &self,
        ctx: &ExecutionContext,
        name: String,
        if_exists: bool,
        change: crate::sequences::SequenceChange,
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, sequence) = parse_object_name(&name)?;
        let tbl = match self
            .catalog_reader
            .get_table(db_name, schema_name, sequence)
        {
            Ok(tbl) if crate::sequences::is_sequence(&tbl) => tbl,
            Ok(_) => anyhow::bail!("\"{sequence}\" is not a sequence"),
            Err(_) if if_exists => {
                self.notice(
                    ctx,
                    DbError::new(format!("relation \"{sequence}\" does not exist, skipping")),
                );
                return Ok(QueryOutput::tag("ALTER SEQUENCE"));
            }
            Err(_) => anyhow::bail!("relation \"{sequence}\" does not exist"),
        };
        self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
        self.sequences.alter(&name, &change)?;
        if let Some(new_name) = change.rename {
            if self
                .catalog_reader
                .get_table(db_name, schema_name, &new_name)
                .is_ok()
            {
                anyhow::bail!("relation \"{new_name}\" already exists");
            }
            self.catalog_writer.update_table_descriptor(
                nodus_catalog::TableDescriptorChange::RenameTable {
                    table_id: tbl.id,
                    new_name,
                },
            )?;
        }
        Ok(QueryOutput::tag("ALTER SEQUENCE"))
    }

    /// `CREATE TABLE ... AS <query>` / `SELECT ... INTO` / `CREATE
    /// MATERIALIZED VIEW`: runs the query, then creates a table with its
    /// output columns and types and, unless `WITH NO DATA`, inserts its rows.
    /// The command tag is `SELECT <n>`, as in PostgreSQL. A materialized view
    /// keeps its query for `REFRESH`.
    /// The columns `CREATE TABLE ... (LIKE source)` copies: names, types,
    /// and NOT NULL, and with `INCLUDING DEFAULTS` their defaults (an
    /// identity column's generator is not one).
    pub(crate) fn like_columns(&self, source: &str, defaults: bool) -> Result<Vec<ColumnDef>> {
        let (db_name, schema_name, table_only) = parse_object_name(source)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        Ok(tbl
            .columns
            .iter()
            .map(|c| ColumnDef {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
                unique: false,
                primary: false,
                default: Self::column_default(c).filter(|d| {
                    defaults
                        && !matches!(d, ScalarExpr::Function { name, .. } if name == "__IDENTITY__")
                }),
                sequence: None,
            })
            .collect())
    }

    pub(crate) fn exec_create_table_as(
        &self,
        ctx: &ExecutionContext,
        name: String,
        query: LogicalPlan,
        if_not_exists: bool,
        (with_data, materialized): (bool, bool),
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, table_only) = parse_object_name(&name)?;
        // A temporary relation's schema is made for the session on first use.
        let temp = crate::search_path::is_temp_schema(schema_name);
        if temp {
            self.ensure_temp_schema(ctx)?;
        }
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
        self.reject_type_name(schema_name, table_only)?;
        let materialized_query = if materialized {
            Some(self.bind_view_plan(&serde_json::to_string(&query)?))
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
            Vec::new(),
            (None, None, None),
        )?;
        if !with_data {
            return Ok(QueryOutput::tag(command));
        }
        let rows: Vec<Vec<Value>> = out.rows.into_iter().map(|r| r.values).collect();
        let count = rows.len();
        if count > 0 {
            self.exec_insert(
                ctx,
                name,
                vec![],
                rows,
                Default::default(),
                None,
                (vec![], None, None),
            )?;
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
        match kind {
            "SCHEMA" => return self.exec_comment_schema(ctx, relation, comment),
            "TYPE" | "DOMAIN" => return self.exec_comment_type(ctx, kind, relation, comment),
            _ => {}
        }
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
            self.exec_insert(
                ctx,
                name,
                vec![],
                rows,
                Default::default(),
                None,
                (vec![], None, None),
            )?;
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
        // A temporary relation's schema is made for the session on first use.
        let temp = crate::search_path::is_temp_schema(schema_name);
        if temp {
            self.ensure_temp_schema(ctx)?;
        }
        let db = self.catalog_reader.get_database(db_name)?;
        let sch = self.catalog_reader.get_schema(db_name, schema_name)?;
        if !temp {
            self.authorize(ctx, Action::CreateTable, ResourceRef::Schema(sch.id))?;
        }
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
            Some(_) => {}
            None => self.reject_type_name(schema_name, view_only)?,
        }

        // The view's columns are its query's; stored, the query runs on
        // every read.
        let view_query_json = self.bind_view_plan(&serde_json::to_string(&*query)?);
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
            parents: Vec::new(),
            partition_by: None,
            partition_bound: None,
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
        // A partitioned table takes its partitions with it.
        for target in targets.clone() {
            if target.partition_by.is_some() {
                for descendant in self.descendants("default", target.id)? {
                    if !targets.iter().any(|t| t.id == descendant.id) {
                        targets.push(descendant);
                    }
                }
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
                Dependent::ChildTable(child) => {
                    // The child's own descendants go first, deepest last; one
                    // an earlier dependent already dropped is skipped.
                    for descendant in self.descendants("default", child.id)?.into_iter().rev() {
                        if self.catalog_reader.get_table_by_id(descendant.id).is_ok() {
                            self.drop_relation(&descendant)?;
                        }
                    }
                    if self.catalog_reader.get_table_by_id(child.id).is_ok() {
                        self.drop_relation(child)?;
                    }
                }
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
    pub(crate) fn drop_relation(&self, tbl: &nodus_catalog::TableDescriptor) -> Result<()> {
        self.catalog_writer.drop_table(tbl.id)?;
        for column in &tbl.columns {
            let Some(sequence) = crate::sequences::owned_by_column(&tbl.name, column) else {
                continue;
            };
            // An unqualified name is the table's schema's.
            let sequence = if sequence.contains('.') {
                sequence
            } else {
                format!("{}.{sequence}", self.schema_name_of(tbl))
            };
            let (db, schema, seq) = parse_object_name(&sequence)?;
            if let Ok(seq_tbl) = self.catalog_reader.get_table(db, schema, seq)
                && crate::sequences::is_sequence(&seq_tbl)
            {
                self.catalog_writer.drop_table(seq_tbl.id)?;
            }
        }
        Ok(())
    }

    /// The objects depending on `targets` that are not among them, as
    /// PostgreSQL lists them: the foreign keys referencing each target and
    /// the views reading it, each such view followed by those reading it.
    pub(crate) fn dependents(
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
            // A child table depends on its parent.
            for child in self.descendants("default", target.id)? {
                if targets.iter().any(|t| t.id == child.id) || seen.contains(&child.id) {
                    continue;
                }
                seen.push(child.id);
                let parent = child
                    .parents
                    .first()
                    .and_then(|id| tables.iter().find(|t| t.id == *id))
                    .map(|t| t.name.clone())
                    .unwrap_or_else(|| target.name.clone());
                found.push(DependentObject {
                    description: format!("table {} depends on table {}", child.name, parent),
                    object: Dependent::ChildTable(child),
                });
            }
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

    /// `CREATE [UNIQUE] INDEX [name] ON table (columns) [WHERE predicate]`.
    pub(crate) fn exec_create_index(
        &self,
        ctx: &ExecutionContext,
        name: String,
        table_name: String,
        (columns, expressions, descending): (Vec<String>, Vec<Option<String>>, Vec<bool>),
        (unique, nulls_not_distinct, predicate): (bool, bool, Option<String>),
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
        // Key parts that are expressions have no column; the rest are
        // looked up by name.
        let plain: Vec<String> = columns
            .iter()
            .zip(expressions.iter().chain(std::iter::repeat(&None)))
            .filter(|(_, e)| e.is_none())
            .map(|(c, _)| c.clone())
            .collect();
        // A geometric column has no default operator class for btree.
        for column in &plain {
            let data_type = tbl
                .columns
                .iter()
                .find(|c| c.name == *column)
                .map(|c| c.data_type.clone())
                .unwrap_or_default();
            let no_btree_class = crate::geometric::Kind::of(&data_type)
                .map(|kind| kind.name().to_string())
                .or_else(|| crate::jsonpath::is_type(&data_type).then(|| "jsonpath".to_string()));
            if let Some(name) = no_btree_class {
                anyhow::bail!(
                    "{}",
                    crate::error_fields::DbError::new(format!(
                        "data type {} has no default operator class for access method \"btree\"",
                        name
                    ))
                    .code("42704")
                    .hint(
                        "You must specify an operator class for the index or define a default operator class for the data type."
                    )
                    .into_text()
                );
            }
        }
        let mut index = Self::new_index(&tbl, name, index_type, &plain, predicate, false)?;
        index.nulls_not_distinct = nulls_not_distinct;
        if expressions.iter().any(Option::is_some) {
            let mut plain_keys = index.key_columns.into_iter();
            index.key_columns = expressions
                .iter()
                .map(|e| match e {
                    Some(_) => Some(nodus_catalog::IndexColumn {
                        column_id: crate::index_keys::EXPRESSION_KEY,
                        descending: false,
                    }),
                    None => plain_keys.next(),
                })
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default();
            index.expressions = expressions
                .iter()
                .flatten()
                .map(|sql| nodus_catalog::Expression { sql: sql.clone() })
                .collect();
        }
        for (key, descending) in index.key_columns.iter_mut().zip(&descending) {
            key.descending = *descending;
        }
        // A unique index on a partitioned table must cover its key.
        if unique && let Some(key_text) = &tbl.partition_by {
            let key = crate::partitioning::PartitionKey::parse(key_text)?;
            if let Some(missing) = key.columns.iter().find(|column| !plain.contains(column)) {
                anyhow::bail!(DbError::new(
                    "unique constraint on partitioned table must include all partitioning columns"
                )
                .detail(format!(
                    "UNIQUE constraint on table \"{table_only}\" lacks column \"{missing}\" which is part of the partition key."
                ))
                .into_text());
            }
        }
        self.add_index(ctx, &tbl, index)?;
        // A partitioned table's new index reaches the partitions it has.
        if tbl.partition_by.is_some() {
            let fresh = self.catalog_reader.get_table_by_id(tbl.id)?;
            for partition in self.partition_children(db_name, tbl.id)? {
                self.create_partition_indexes(&partition, &fresh)?;
            }
        }
        Ok(QueryOutput::tag("CREATE INDEX"))
    }

    /// An index of `tbl` named `name` over `columns`.
    pub(crate) fn new_index(
        tbl: &nodus_catalog::TableDescriptor,
        name: String,
        index_type: nodus_catalog::IndexType,
        columns: &[String],
        predicate: Option<String>,
        constraint: bool,
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
            constraint,
            deferrable: false,
            initially_deferred: false,
            nulls_not_distinct: false,
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
            let predicate = index.predicate.as_ref().map(|p| p.sql.as_str());
            self.check_unique_key(ctx, tbl, &index, predicate)?;
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
        index: &nodus_catalog::IndexDescriptor,
        predicate: Option<&str>,
    ) -> Result<()> {
        let name = &index.name;
        let predicate = match predicate {
            Some(sql) => Some(self.index_predicate(sql)?),
            None => None,
        };
        let names: Vec<String> = tbl.columns.iter().map(|c| c.name.clone()).collect();
        let parts = crate::index_keys::index_parts(tbl, index);
        let mut seen = std::collections::HashSet::new();
        for row in self.scan_rows(tbl.id, &ctx.session_id)? {
            if let Some(filter) = &predicate
                && self.eval_filter(ctx, &row, &names, &tbl.columns, Some(filter)) != Some(true)
            {
                continue;
            }
            let key = crate::index_keys::key_values(tbl, &parts, &row);
            if key.iter().any(|v| matches!(v, Value::Null)) {
                continue;
            }
            let rendered: Vec<String> = key
                .iter()
                .map(|v| render(&crate::value::key_form(v)))
                .collect();
            if !seen.insert(rendered.join("\u{1}")) {
                let columns = crate::index_keys::key_names(tbl, &parts);
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
        self.catalog_writer.update_table_descriptor(
            nodus_catalog::TableDescriptorChange::AddIndex {
                table_id: tbl.id,
                index: index.clone(),
            },
        )?;
        let prefix = format!("{}:", tbl.id);
        for (key, row) in self.scan_rows_keyed(tbl.id, &ctx.session_id)? {
            let pk = key.strip_prefix(&prefix).unwrap_or(&key);
            if let Some(value) = Self::index_leading_value(tbl, &index, &row) {
                self.write_index_entry(&ctx.session_id, index.id, &value, pk)?;
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
    pub(crate) fn of(tbl: &nodus_catalog::TableDescriptor) -> Option<RelationKind> {
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

    pub(crate) fn noun(self) -> &'static str {
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
pub(crate) struct DependentObject {
    /// As PostgreSQL's DETAIL lists it: `view v depends on table t`.
    pub(crate) description: String,
    pub(crate) object: Dependent,
}

impl DependentObject {
    /// The object as a notice names it: `view v`, `constraint c on table t`.
    pub(crate) fn object_description(&self) -> String {
        self.description
            .split(" depends on ")
            .next()
            .unwrap_or(&self.description)
            .to_string()
    }
}

pub(crate) enum Dependent {
    /// A foreign key of another table.
    Constraint {
        table: nodus_catalog::TableId,
        name: String,
    },
    View(nodus_catalog::TableDescriptor),
    /// A child table (`INHERITS`).
    ChildTable(nodus_catalog::TableDescriptor),
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
                let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
                let name = check_constraint_name(table, &column_names, &expr, &taken);
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

/// The name PostgreSQL gives an unnamed CHECK constraint `expr` of `table`:
/// `<table>_<column>_check` when it names one of `columns`, else
/// `<table>_check`, numbered when the name is `taken`.
pub(crate) fn check_constraint_name(
    table: &str,
    columns: &[String],
    expr: &str,
    taken: &[String],
) -> String {
    let mut named: Vec<&str> = expr
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .filter(|word| columns.iter().any(|c| c == word))
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
    name
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
            sample,
            ..
        } => LogicalPlan::Select {
            only: false,
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
            sample,
        },
        other => other,
    }
}
