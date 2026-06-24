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
            } => {
                if table_name.eq_ignore_ascii_case("pg_stat_ssl") {
                    return Ok(QueryOutput {
                        columns: vec!["ssl".to_string()],
                        types: vec!["BOOL".to_string()],
                        rows: vec![Row {
                            values: vec![Value::Bool(false)],
                        }],
                        tag: "SELECT 1".into(),
                    });
                }

                let mut cte_results = std::collections::HashMap::new();
                for (name, cte_plan) in ctes {
                    let out = self.execute_logical_inner(ctx, *cte_plan)?;
                    cte_results.insert(name, out);
                }

                let (tbl_cols, mut col_names, mut stored_rows) = if let Some(cte_out) =
                    cte_results.get(&table_name)
                {
                    let mut cols = Vec::new();
                    for (i, c) in cte_out.columns.iter().enumerate() {
                        let ty = cte_out
                            .types
                            .get(i)
                            .unwrap_or(&"VARCHAR".to_string())
                            .clone();
                        cols.push(ColumnDescriptor {
                            id: nodus_catalog::ColumnId::new(),
                            name: c.clone(),
                            version: 1,
                            created_at: chrono::Utc::now(),
                            updated_at: chrono::Utc::now(),
                            state: nodus_catalog::DescriptorState::Public,
                            data_type: ty,
                            nullable: true,
                        });
                    }
                    let prefix = table_alias.as_deref().unwrap_or(&table_name);
                    let col_names = cols
                        .iter()
                        .map(|c| format!("{}.{}", prefix, c.name))
                        .collect();
                    (
                        cols,
                        col_names,
                        Some(cte_out.rows.iter().map(|r| r.values.clone()).collect()),
                    )
                } else {
                    let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                    let schema_name = if schema_name.eq_ignore_ascii_case("public")
                        && Self::is_pg_catalog_virtual_table_name(table_only)
                    {
                        "pg_catalog"
                    } else {
                        schema_name
                    };
                    let (tbl_cols, col_names, rows) = if Self::is_virtual_schema(schema_name) {
                        let (cols, rows) =
                            self.get_virtual_table(db_name, schema_name, table_only)?;
                        let prefix = table_alias.as_deref().unwrap_or(&table_name);
                        let col_names: Vec<String> = cols
                            .iter()
                            .map(|c| format!("{}.{}", prefix, c.name))
                            .collect();
                        (cols, col_names, rows)
                    } else {
                        let tbl =
                            self.catalog_reader
                                .get_table(db_name, schema_name, table_only)?;
                        self.authorize(ctx, Action::Select, ResourceRef::Table(tbl.id))?;

                        let prefix = table_alias.as_deref().unwrap_or(&table_name);
                        let col_names: Vec<String> = tbl
                            .columns
                            .iter()
                            .map(|c| format!("{}.{}", prefix, c.name))
                            .collect();

                        let mut rows = None;
                        let has_session_overlay = self
                            .active_txns
                            .read()
                            .unwrap()
                            .get(&ctx.session_id)
                            .map(|txn| !txn.overlay.is_empty())
                            .unwrap_or(false);
                        if !has_session_overlay
                            && let Some(FilterExpr::Predicate(Predicate {
                                left,
                                op: CompareOp::Eq,
                                right,
                            })) = filter.as_ref()
                        {
                            let col_name = left.split('.').last().unwrap_or(left);
                            if let Some(col) = tbl.columns.iter().find(|c| c.name == *col_name) {
                                for idx in &tbl.indexes {
                                    if idx.key_columns.iter().any(|kc| kc.column_id == col.id) {
                                        let val =
                                            self.eval_operand(&[], &[], &[], right, &col.data_type);
                                        if let Ok(indexed_rows) =
                                            self.index_scan(idx.id, &val, tbl.id, &ctx.session_id)
                                        {
                                            rows = Some(indexed_rows);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        let rows = match rows {
                            Some(r) => r,
                            None => {
                                if let Some(vq) = &tbl.view_query {
                                    let plan: LogicalPlan = serde_json::from_str(vq)?;
                                    let out = self.execute_logical_inner(ctx, plan)?;
                                    out.rows.iter().map(|r| r.values.clone()).collect()
                                } else {
                                    self.scan_rows(tbl.id, &ctx.session_id)?
                                }
                            }
                        };
                        (tbl.columns.clone(), col_names, rows)
                    };
                    (tbl_cols, col_names, Some(rows))
                };

                let mut joined_columns = tbl_cols;
                let mut stored_rows = stored_rows.unwrap();

                for join in &joins {
                    let (j_cols, j_rows) = if let Some(cte_out) = cte_results.get(&join.table_name)
                    {
                        let mut cols = Vec::new();
                        for (i, c) in cte_out.columns.iter().enumerate() {
                            let ty = cte_out
                                .types
                                .get(i)
                                .unwrap_or(&"VARCHAR".to_string())
                                .clone();
                            cols.push(ColumnDescriptor {
                                id: nodus_catalog::ColumnId::new(),
                                name: c.clone(),
                                version: 1,
                                created_at: chrono::Utc::now(),
                                updated_at: chrono::Utc::now(),
                                state: nodus_catalog::DescriptorState::Public,
                                data_type: ty,
                                nullable: true,
                            });
                        }
                        (
                            cols,
                            cte_out.rows.iter().map(|r| r.values.clone()).collect(),
                        )
                    } else {
                        let (j_db, j_sch, j_tbl_name) = parse_object_name(&join.table_name)?;
                        let j_sch = if j_sch.eq_ignore_ascii_case("public")
                            && Self::is_pg_catalog_virtual_table_name(j_tbl_name)
                        {
                            "pg_catalog"
                        } else {
                            j_sch
                        };
                        if Self::is_virtual_schema(j_sch) {
                            let (cols, rows) = self.get_virtual_table(j_db, j_sch, j_tbl_name)?;
                            (cols, rows)
                        } else {
                            let j_tbl = self.catalog_reader.get_table(j_db, j_sch, j_tbl_name)?;
                            self.authorize(ctx, Action::Select, ResourceRef::Table(j_tbl.id))?;
                            let j_rows = if let Some(vq) = &j_tbl.view_query {
                                let plan: LogicalPlan = serde_json::from_str(vq)?;
                                let out = self.execute_logical_inner(ctx, plan)?;
                                out.rows.iter().map(|r| r.values.clone()).collect()
                            } else {
                                self.scan_rows(j_tbl.id, &ctx.session_id)?
                            };
                            (j_tbl.columns.clone(), j_rows)
                        }
                    };

                    let j_prefix = join.table_alias.as_deref().unwrap_or(&join.table_name);
                    let j_col_names: Vec<String> = j_cols
                        .iter()
                        .map(|c| format!("{}.{}", j_prefix, c.name))
                        .collect();

                    let mut combined_cols = col_names.clone();
                    combined_cols.extend(j_col_names.clone());

                    let mut combined_desc = joined_columns.clone();
                    combined_desc.extend(j_cols.clone());

                    let mut next_rows = Vec::new();
                    let mut right_matched = vec![false; j_rows.len()];
                    for r1 in &stored_rows {
                        let mut matched = false;
                        for (j_idx, r2) in j_rows.iter().enumerate() {
                            let mut combined_row = r1.clone();
                            combined_row.extend(r2.clone());
                            if self
                                .eval_filter(
                                    ctx,
                                    &combined_row,
                                    &combined_cols,
                                    &combined_desc,
                                    join.condition.as_ref(),
                                )
                                .unwrap_or(false)
                            {
                                next_rows.push(combined_row);
                                matched = true;
                                right_matched[j_idx] = true;
                            }
                        }
                        if !matched
                            && matches!(join.join_type, JoinType::LeftOuter | JoinType::FullOuter)
                        {
                            let mut combined_row = r1.clone();
                            // Left or Full join requires filling the right side with NULLs
                            let num_nulls = j_cols.len();
                            combined_row.extend(vec![Value::Null; num_nulls]);
                            next_rows.push(combined_row);
                        }
                    }
                    if matches!(join.join_type, JoinType::RightOuter | JoinType::FullOuter) {
                        let left_len = col_names.len();
                        for (j_idx, matched) in right_matched.into_iter().enumerate() {
                            if !matched {
                                let mut combined_row = vec![Value::Null; left_len];
                                combined_row.extend(j_rows[j_idx].clone());
                                next_rows.push(combined_row);
                            }
                        }
                    }
                    stored_rows = next_rows;
                    col_names = combined_cols;
                    joined_columns = combined_desc;
                }

                // WHERE: conjunction of typed predicates.
                stored_rows.retain(|r| {
                    self.eval_filter(ctx, r, &col_names, &joined_columns, filter.as_ref())
                        .unwrap_or(false)
                });

                // GROUP BY & Aggregation
                let is_agg = !group_by.is_empty()
                    || projection
                        .iter()
                        .any(|p| matches!(p, ProjectionItem::Aggregate(_, _)));

                if !is_agg {
                    if !order_by.is_empty() {
                        let mut order_indices = Vec::new();
                        for (ocol, asc) in &order_by {
                            let idx = col_names
                                .iter()
                                .position(|c| c == ocol || c.ends_with(&format!(".{}", ocol)));
                            if let Some(i) = idx {
                                order_indices.push((i, *asc));
                            }
                        }
                        stored_rows.sort_by(|a, b| {
                            for (idx, asc) in &order_indices {
                                let ord = compare(
                                    a.get(*idx).unwrap_or(&crate::Value::Null),
                                    b.get(*idx).unwrap_or(&crate::Value::Null),
                                );
                                if ord != std::cmp::Ordering::Equal {
                                    return if *asc { ord } else { ord.reverse() };
                                }
                            }
                            std::cmp::Ordering::Equal
                        });
                    }
                }

                let mut out_rows = Vec::new();
                let mut out_cols = Vec::new();

                if is_agg {
                    let mut groups: std::collections::BTreeMap<Vec<Vec<u8>>, Vec<Vec<Value>>> =
                        std::collections::BTreeMap::new();

                    let group_by_indices: Vec<Option<usize>> = group_by
                        .iter()
                        .map(|c| {
                            col_names
                                .iter()
                                .position(|tc| tc == c || tc.ends_with(&format!(".{}", c)))
                        })
                        .collect();

                    if stored_rows.is_empty() && group_by.is_empty() {
                        // Empty set but scalar agg like COUNT(*), yields one row
                        groups.insert(vec![], vec![]);
                    } else {
                        for r in stored_rows {
                            let key = group_by_indices
                                .iter()
                                .map(|i| {
                                    let val = i.and_then(|idx| r.get(idx)).unwrap_or(&Value::Null);
                                    // serialize for BTreeMap key
                                    serde_json::to_vec(val).unwrap_or_default()
                                })
                                .collect::<Vec<_>>();
                            groups.entry(key).or_default().push(r);
                        }
                    }

                    for (_k, group_rows) in groups {
                        // HAVING filters whole groups after aggregation.
                        if let Some(h) = having.as_ref() {
                            if !eval_having(h, &group_rows, &col_names) {
                                continue;
                            }
                        }
                        let mut out_row = Vec::new();
                        for proj_item in &projection {
                            match proj_item {
                                ProjectionItem::Literal(v)
                                | ProjectionItem::AliasedLiteral(v, _) => {
                                    out_row.push(v.clone());
                                }
                                ProjectionItem::Column(c) | ProjectionItem::AliasedColumn(c, _) => {
                                    let idx = col_names
                                        .iter()
                                        .position(|tc| tc == c || tc.ends_with(&format!(".{}", c)));
                                    // Take from first row of group
                                    out_row.push(
                                        group_rows
                                            .first()
                                            .and_then(|r| idx.and_then(|i| r.get(i)))
                                            .map(|v| v.clone())
                                            .unwrap_or(crate::Value::Null),
                                    );
                                }
                                ProjectionItem::WindowFunction { .. }
                                | ProjectionItem::ScalarFunction { .. }
                                | ProjectionItem::JsonAccess { .. }
                                | ProjectionItem::CaseWhenEq { .. } => {
                                    out_row.push(Value::Null); // MVP fallback
                                }
                                ProjectionItem::Aggregate(op, inner) => {
                                    out_row.push(compute_aggregate(
                                        op,
                                        inner,
                                        &group_rows,
                                        &col_names,
                                    ));
                                }
                            }
                        }
                        out_rows.push(out_row);
                    }

                    out_cols = if projection.is_empty() {
                        col_names.clone()
                    } else {
                        projection
                            .iter()
                            .map(|p| match p {
                                ProjectionItem::Column(c) => {
                                    c.split('.').last().unwrap_or(c).to_string()
                                }
                                ProjectionItem::AliasedColumn(_, a) => a.clone(),
                                ProjectionItem::Literal(_) => "?column?".to_string(),
                                ProjectionItem::AliasedLiteral(_, a) => a.clone(),
                                ProjectionItem::WindowFunction {
                                    func_name, alias, ..
                                } => alias.clone().unwrap_or_else(|| func_name.clone()),
                                ProjectionItem::ScalarFunction {
                                    func_name, alias, ..
                                } => alias.clone().unwrap_or_else(|| func_name.clone()),
                                ProjectionItem::JsonAccess {
                                    left,
                                    operator,
                                    right,
                                    alias,
                                } => alias
                                    .clone()
                                    .unwrap_or_else(|| format!("{}{}{}", left, operator, right)),
                                ProjectionItem::CaseWhenEq {
                                    else_column, alias, ..
                                } => alias.clone().unwrap_or_else(|| {
                                    else_column
                                        .split('.')
                                        .last()
                                        .unwrap_or(else_column)
                                        .to_string()
                                }),
                                ProjectionItem::Aggregate(op, inner) => {
                                    format!("{:?}({})", op, inner)
                                }
                            })
                            .collect()
                    };
                } else {
                    out_cols =
                        if projection.is_empty() {
                            col_names
                                .iter()
                                .map(|c| c.split('.').last().unwrap_or(c).to_string())
                                .collect()
                        } else {
                            projection
                                .iter()
                                .filter_map(|p| match p {
                                    ProjectionItem::Column(c) => {
                                        Some(c.split('.').last().unwrap_or(c).to_string())
                                    }
                                    ProjectionItem::AliasedColumn(_, a) => Some(a.clone()),
                                    ProjectionItem::Literal(_) => Some("?column?".to_string()),
                                    ProjectionItem::AliasedLiteral(_, a) => Some(a.clone()),
                                    ProjectionItem::WindowFunction {
                                        alias, func_name, ..
                                    } => Some(alias.clone().unwrap_or_else(|| func_name.clone())),
                                    ProjectionItem::ScalarFunction {
                                        alias, func_name, ..
                                    } => Some(alias.clone().unwrap_or_else(|| func_name.clone())),
                                    ProjectionItem::JsonAccess {
                                        left,
                                        operator,
                                        right,
                                        alias,
                                    } => Some(alias.clone().unwrap_or_else(|| {
                                        format!("{}{}{}", left, operator, right)
                                    })),
                                    ProjectionItem::CaseWhenEq {
                                        else_column, alias, ..
                                    } => Some(alias.clone().unwrap_or_else(|| {
                                        else_column
                                            .split('.')
                                            .last()
                                            .unwrap_or(else_column)
                                            .to_string()
                                    })),
                                    _ => None,
                                })
                                .collect()
                        };

                    // Evaluate Window Functions and Scalar Expressions before projecting
                    for proj_item in projection.iter() {
                        match proj_item {
                            ProjectionItem::WindowFunction {
                                func_name,
                                partition_by,
                                order_by: w_order_by,
                                alias,
                            } => {
                                let p_indices: Vec<usize> = partition_by
                                    .iter()
                                    .filter_map(|c| {
                                        col_names.iter().position(|tc| {
                                            tc == c || tc.ends_with(&format!(".{}", c))
                                        })
                                    })
                                    .collect();

                                let o_indices: Vec<(usize, bool)> = w_order_by
                                    .iter()
                                    .filter_map(|(c, asc)| {
                                        col_names
                                            .iter()
                                            .position(|tc| {
                                                tc == c || tc.ends_with(&format!(".{}", c))
                                            })
                                            .map(|idx| (idx, *asc))
                                    })
                                    .collect();

                                let mut row_indices: Vec<usize> = (0..stored_rows.len()).collect();
                                row_indices.sort_by(|&a_idx, &b_idx| {
                                    let a = &stored_rows[a_idx];
                                    let b = &stored_rows[b_idx];
                                    for &p_idx in &p_indices {
                                        let cmp = compare(
                                            a.get(p_idx).unwrap_or(&Value::Null),
                                            b.get(p_idx).unwrap_or(&Value::Null),
                                        );
                                        if cmp != std::cmp::Ordering::Equal {
                                            return cmp;
                                        }
                                    }
                                    for &(o_idx, asc) in &o_indices {
                                        let mut cmp = compare(
                                            a.get(o_idx).unwrap_or(&Value::Null),
                                            b.get(o_idx).unwrap_or(&Value::Null),
                                        );
                                        if !asc {
                                            cmp = cmp.reverse();
                                        }
                                        if cmp != std::cmp::Ordering::Equal {
                                            return cmp;
                                        }
                                    }
                                    std::cmp::Ordering::Equal
                                });

                                let mut results = vec![Value::Null; stored_rows.len()];
                                if func_name == "ROW_NUMBER" {
                                    let mut current_partition: Vec<Value> = Vec::new();
                                    let mut row_num = 1i64;
                                    for &row_idx in &row_indices {
                                        let row = &stored_rows[row_idx];
                                        let partition_key: Vec<Value> = p_indices
                                            .iter()
                                            .map(|&idx| {
                                                row.get(idx).unwrap_or(&Value::Null).clone()
                                            })
                                            .collect();
                                        if partition_key != current_partition {
                                            current_partition = partition_key;
                                            row_num = 1;
                                        }
                                        results[row_idx] = Value::Int(row_num);
                                        row_num += 1;
                                    }
                                } else {
                                    anyhow::bail!("Unsupported window function: {}", func_name);
                                }

                                // Append the result to `stored_rows`
                                for (row_idx, row) in stored_rows.iter_mut().enumerate() {
                                    row.push(results[row_idx].clone());
                                }
                                let w_col_name = alias.clone().unwrap_or_else(|| func_name.clone());
                                col_names.push(w_col_name);
                            }
                            ProjectionItem::ScalarFunction {
                                func_name,
                                args,
                                alias,
                            } => {
                                let mut results = vec![Value::Null; stored_rows.len()];
                                for (row_idx, row) in stored_rows.iter().enumerate() {
                                    let resolved: Vec<Value> = args
                                        .iter()
                                        .map(|a| resolve_scalar_arg(a, row, &col_names))
                                        .collect();
                                    results[row_idx] = eval_scalar_function(func_name, &resolved);
                                }
                                for (row_idx, row) in stored_rows.iter_mut().enumerate() {
                                    row.push(results[row_idx].clone());
                                }
                                let w_col_name = alias.clone().unwrap_or_else(|| func_name.clone());
                                col_names.push(w_col_name);
                            }
                            ProjectionItem::JsonAccess {
                                left,
                                operator,
                                right,
                                alias,
                            } => {
                                let mut results = vec![Value::Null; stored_rows.len()];
                                for (row_idx, row) in stored_rows.iter().enumerate() {
                                    let c_idx = col_names.iter().position(|tc| {
                                        tc == left || tc.ends_with(&format!(".{}", left))
                                    });
                                    if let Some(i) = c_idx {
                                        if let Some(v) = row.get(i) {
                                            if operator == "->>" {
                                                let json_str = match v {
                                                    Value::Jsonb(j) => j.to_string(),
                                                    Value::Text(s) => s.clone(),
                                                    _ => "".to_string(),
                                                };
                                                if let Ok(json) =
                                                    serde_json::from_str::<serde_json::Value>(
                                                        &json_str,
                                                    )
                                                {
                                                    if let Some(obj) = json.as_object() {
                                                        if let Some(val) = obj.get(right) {
                                                            results[row_idx] = match val {
                                                                serde_json::Value::String(s) => {
                                                                    Value::Text(s.clone())
                                                                }
                                                                _ => Value::Text(val.to_string()),
                                                            };
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                for (row_idx, row) in stored_rows.iter_mut().enumerate() {
                                    row.push(results[row_idx].clone());
                                }
                                let w_col_name = alias
                                    .clone()
                                    .unwrap_or_else(|| format!("{}{}{}", left, operator, right));
                                col_names.push(w_col_name);
                            }
                            _ => {}
                        }
                    }

                    let indices: Vec<Option<usize>> = out_cols
                        .iter()
                        .enumerate()
                        .map(|(pi, c)| {
                            if projection.is_empty() {
                                // `out_cols` mirrors `col_names` positionally (just unqualified);
                                // resolving by name would collapse duplicate column names across joined tables.
                                Some(pi)
                            } else {
                                let actual_col = match &projection[pi] {
                                    ProjectionItem::Column(c) => c.clone(),
                                    ProjectionItem::AliasedColumn(c, _) => c.clone(),
                                    ProjectionItem::Literal(_)
                                    | ProjectionItem::AliasedLiteral(_, _) => "".to_string(),
                                    ProjectionItem::WindowFunction {
                                        func_name, alias, ..
                                    } => alias.clone().unwrap_or_else(|| func_name.clone()),
                                    ProjectionItem::ScalarFunction {
                                        func_name, alias, ..
                                    } => alias.clone().unwrap_or_else(|| func_name.clone()),
                                    ProjectionItem::JsonAccess {
                                        left,
                                        operator,
                                        right,
                                        alias,
                                    } => alias.clone().unwrap_or_else(|| {
                                        format!("{}{}{}", left, operator, right)
                                    }),
                                    ProjectionItem::CaseWhenEq {
                                        else_column, alias, ..
                                    } => alias.clone().unwrap_or_else(|| else_column.clone()),
                                    _ => c.clone(),
                                };
                                col_names.iter().position(|tc| {
                                    tc == &actual_col || tc.ends_with(&format!(".{}", actual_col))
                                })
                            }
                        })
                        .collect();

                    out_rows = stored_rows
                        .into_iter()
                        .map(|r| {
                            if projection.is_empty() {
                                indices
                                    .iter()
                                    .map(|i| {
                                        i.and_then(|idx| r.get(idx))
                                            .cloned()
                                            .unwrap_or(crate::Value::Null)
                                    })
                                    .collect()
                            } else {
                                projection
                                    .iter()
                                    .enumerate()
                                    .map(|(pi, proj)| match proj {
                                        ProjectionItem::Literal(v)
                                        | ProjectionItem::AliasedLiteral(v, _) => v.clone(),
                                        ProjectionItem::CaseWhenEq {
                                            left,
                                            equals,
                                            then_value,
                                            then_column,
                                            else_column,
                                            ..
                                        } => {
                                            let left_idx = col_names.iter().position(|tc| {
                                                tc == left || tc.ends_with(&format!(".{}", left))
                                            });
                                            let else_idx = col_names.iter().position(|tc| {
                                                tc == else_column
                                                    || tc.ends_with(&format!(".{}", else_column))
                                            });
                                            let left_value = left_idx
                                                .and_then(|idx| r.get(idx))
                                                .unwrap_or(&Value::Null);
                                            if compare(left_value, equals)
                                                == std::cmp::Ordering::Equal
                                            {
                                                if let Some(then_column) = then_column {
                                                    col_names
                                                        .iter()
                                                        .position(|tc| {
                                                            tc == then_column
                                                                || tc.ends_with(&format!(
                                                                    ".{}",
                                                                    then_column
                                                                ))
                                                        })
                                                        .and_then(|idx| r.get(idx))
                                                        .cloned()
                                                        .unwrap_or(Value::Null)
                                                } else {
                                                    then_value.clone()
                                                }
                                            } else {
                                                else_idx
                                                    .and_then(|idx| r.get(idx))
                                                    .cloned()
                                                    .unwrap_or(Value::Null)
                                            }
                                        }
                                        _ => indices[pi]
                                            .and_then(|idx| r.get(idx))
                                            .cloned()
                                            .unwrap_or(crate::Value::Null),
                                    })
                                    .collect()
                            }
                        })
                        .collect::<Vec<_>>();
                }

                // ORDER BY for aggregates (uses out_cols). For non-aggregates, it was already sorted.
                if is_agg {
                    if !order_by.is_empty() {
                        let mut order_indices = Vec::new();
                        for (ocol, asc) in &order_by {
                            let idx = out_cols
                                .iter()
                                .position(|c| c == ocol || c.ends_with(&format!(".{}", ocol)));
                            if let Some(i) = idx {
                                order_indices.push((i, *asc));
                            }
                        }
                        out_rows.sort_by(|a, b| {
                            for (idx, asc) in &order_indices {
                                let ord = compare(
                                    a.get(*idx).unwrap_or(&crate::Value::Null),
                                    b.get(*idx).unwrap_or(&crate::Value::Null),
                                );
                                if ord != std::cmp::Ordering::Equal {
                                    return if *asc { ord } else { ord.reverse() };
                                }
                            }
                            std::cmp::Ordering::Equal
                        });
                    }
                }

                // DISTINCT
                if distinct {
                    let mut seen = Vec::new();
                    out_rows.retain(|r| {
                        let is_seen = seen.iter().any(|s: &Vec<Value>| {
                            s.iter()
                                .zip(r.iter())
                                .all(|(va, vb)| compare(va, vb) == std::cmp::Ordering::Equal)
                        });
                        if is_seen {
                            false
                        } else {
                            seen.push(r.clone());
                            true
                        }
                    });
                }

                // OFFSET
                if let Some(o) = offset {
                    if o < out_rows.len() {
                        out_rows.drain(0..o);
                    } else {
                        out_rows.clear();
                    }
                }

                // LIMIT
                if let Some(n) = limit {
                    out_rows.truncate(n);
                }

                let rows = out_rows
                    .into_iter()
                    .map(|r| Row { values: r })
                    .collect::<Vec<_>>();

                let tag = format!("SELECT {}", rows.len());
                let mut types = Vec::new();
                for (i, c) in out_cols.iter().enumerate() {
                    // Quick lookup for type. Default to VARCHAR.
                    let mut ty = "VARCHAR".to_string();

                    if projection.is_empty() {
                        if let Some(col_desc) = joined_columns.get(i) {
                            ty = col_desc.data_type.clone();
                        }
                    } else if let Some(source_col) = projection.get(i).and_then(|item| match item {
                        ProjectionItem::Column(col) | ProjectionItem::AliasedColumn(col, _) => {
                            Some(col)
                        }
                        _ => None,
                    }) {
                        if let Some(source_idx) = col_names.iter().position(|candidate| {
                            candidate == source_col
                                || candidate.ends_with(&format!(".{}", source_col))
                        }) {
                            if let Some(col_desc) = joined_columns.get(source_idx) {
                                ty = col_desc.data_type.clone();
                            }
                        }
                    }

                    if ty == "VARCHAR" && !rows.is_empty() {
                        if let Some(val) = rows[0].values.get(i) {
                            match val {
                                Value::Int(_) => ty = "INTEGER".to_string(),
                                Value::Float(_) => ty = "DOUBLE".to_string(),
                                Value::Bool(_) => ty = "BOOLEAN".to_string(),
                                Value::Text(_) => ty = "VARCHAR".to_string(),
                                Value::Null => ty = "VARCHAR".to_string(),
                                Value::Array(_) => ty = "VARCHAR".to_string(),
                                Value::Jsonb(_) => ty = "VARCHAR".to_string(),
                            }
                        }
                    } else if ty == "VARCHAR" {
                        // Also try to deduce from projection items if available
                        if i < projection.len() {
                            match &projection[i] {
                                ProjectionItem::Literal(Value::Int(_))
                                | ProjectionItem::AliasedLiteral(Value::Int(_), _) => {
                                    ty = "INTEGER".to_string()
                                }
                                ProjectionItem::Literal(Value::Float(_))
                                | ProjectionItem::AliasedLiteral(Value::Float(_), _) => {
                                    ty = "DOUBLE".to_string()
                                }
                                ProjectionItem::Literal(Value::Bool(_))
                                | ProjectionItem::AliasedLiteral(Value::Bool(_), _) => {
                                    ty = "BOOLEAN".to_string()
                                }
                                _ => {}
                            }
                        }
                    }
                    types.push(ty);
                }
                Ok(QueryOutput {
                    columns: out_cols,
                    types,
                    rows,
                    tag,
                })
            }
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
