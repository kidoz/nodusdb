//! The SELECT executor: CTE materialization, base/virtual-table scan, joins,
//! WHERE filtering, GROUP BY aggregation, HAVING, projection, ORDER BY,
//! DISTINCT, and LIMIT/OFFSET.

use crate::aggregates::*;
use crate::*;
use anyhow::Result;
use bytes::Bytes;
use chrono::Utc;
use nodus_catalog::{ColumnDescriptor, DescriptorState};
use nodus_storage_api::{KeyRange, KvEngine};

impl MemExecutor {
    /// Runs a `WITH RECURSIVE` CTE to a fixpoint: execute the seed once, then
    /// repeatedly run the recursive term against the previous step's rows (the
    /// working table, injected under the CTE name) until it yields nothing new.
    /// UNION dedups against everything accumulated; UNION ALL keeps all rows and
    /// relies on the term's own predicate to terminate.
    fn exec_recursive_cte(
        &self,
        ctx: &ExecutionContext,
        name: &str,
        all: bool,
        column_aliases: Vec<String>,
        seed: LogicalPlan,
        recursive_term: LogicalPlan,
    ) -> Result<QueryOutput> {
        let seed_out = self.execute_logical_inner(ctx, seed)?;
        // Output column names: explicit CTE aliases if given, else the seed's.
        let columns = if column_aliases.is_empty() {
            seed_out.columns.clone()
        } else {
            column_aliases
        };
        let types = seed_out.types.clone();

        let mut result: Vec<Vec<Value>> = seed_out.rows.into_iter().map(|r| r.values).collect();
        let mut working = result.clone();
        let mut guard = 0u32;
        while !working.is_empty() {
            guard += 1;
            if guard > 10_000 {
                anyhow::bail!("WITH RECURSIVE did not terminate within 10000 iterations");
            }
            // The recursive term reads the working table under the CTE name,
            // wherever in the term it refers to it.
            let _working = crate::cte_scope::bind(
                name,
                QueryOutput {
                    columns: columns.clone(),
                    types: types.clone(),
                    rows: working
                        .iter()
                        .cloned()
                        .map(|values| Row { values })
                        .collect(),
                    tag: String::new(),
                },
            );
            let iter = self.execute_logical_inner(ctx, recursive_term.clone())?;

            let mut new_rows: Vec<Vec<Value>> = Vec::new();
            for r in iter.rows {
                let vals = r.values;
                if all {
                    new_rows.push(vals);
                } else if !result.iter().any(|x| x == &vals) && !new_rows.iter().any(|x| x == &vals)
                {
                    new_rows.push(vals);
                }
            }
            result.extend(new_rows.iter().cloned());
            working = new_rows;
        }

        Ok(QueryOutput {
            columns,
            types,
            rows: result.into_iter().map(|values| Row { values }).collect(),
            tag: String::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    /// Computes each CTE and makes it visible to the rest of the statement,
    /// including later CTEs and nested subqueries, until the guards drop.
    pub(crate) fn bind_ctes(
        &self,
        ctx: &ExecutionContext,
        ctes: Vec<(String, Box<LogicalPlan>)>,
    ) -> Result<Vec<crate::cte_scope::ScopeGuard>> {
        let mut bindings = Vec::with_capacity(ctes.len());
        for (name, cte_plan) in ctes {
            let out = match *cte_plan {
                LogicalPlan::RecursiveCte {
                    all,
                    column_aliases,
                    seed,
                    recursive_term,
                } => self.exec_recursive_cte(
                    ctx,
                    &name,
                    all,
                    column_aliases,
                    *seed,
                    *recursive_term,
                )?,
                other => self.execute_logical_inner(ctx, other)?,
            };
            bindings.push(crate::cte_scope::bind(&name, out));
        }
        Ok(bindings)
    }

    /// The joined rows of the relations a data-modifying statement reads
    /// (planned as a `SELECT *` over them), under their qualified names.
    pub(crate) fn relation_rows(
        &self,
        ctx: &ExecutionContext,
        plan: LogicalPlan,
    ) -> Result<QueryOutput> {
        let LogicalPlan::Select {
            ctes,
            table_name,
            table_alias,
            joins,
            filter,
            ..
        } = plan
        else {
            anyhow::bail!("unsupported relation");
        };
        self.exec_select(
            ctx,
            ctes,
            table_name,
            table_alias,
            joins,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            filter,
            None,
            None,
            Vec::new(),
            None,
            None,
            false,
            Vec::new(),
            true,
        )
    }

    pub(crate) fn exec_select(
        &self,
        ctx: &ExecutionContext,
        ctes: Vec<(String, Box<LogicalPlan>)>,
        table_name: String,
        table_alias: Option<String>,
        joins: Vec<Join>,
        projection: Vec<ProjectionItem>,
        group_by: Vec<String>,
        group_exprs: Vec<(String, ScalarExpr)>,
        filter: Option<FilterExpr>,
        having: Option<FilterExpr>,
        grouping_sets: Option<Vec<Vec<String>>>,
        sort: Vec<SortKey>,
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: bool,
        distinct_on: Vec<SortTarget>,
        raw: bool,
    ) -> Result<QueryOutput> {
        // The keys each output row is sorted by, then deduplicated on.
        let key_targets: Vec<SortTarget> = sort
            .iter()
            .map(|k| k.target.clone())
            .chain(distinct_on.iter().cloned())
            .collect();
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

        let _cte_bindings = self.bind_ctes(ctx, ctes)?;

        // LIMIT/OFFSET push-down: when the pipeline is a plain row-by-row scan of
        // a single base table — no join, grouping, ordering, DISTINCT, HAVING,
        // WHERE, or aggregate/window projection — the result is just the first
        // `offset + limit` rows in scan order, so we can stop scanning there
        // instead of materializing the whole table. Any of those operators needs
        // the full input, so they disable the push-down.
        let scan_cap: Option<usize> = match limit {
            Some(lim)
                if joins.is_empty()
                    && group_by.is_empty()
                    && key_targets.is_empty()
                    && having.is_none()
                    && filter.is_none()
                    && !distinct
                    && projection.iter().all(|p| match p {
                        ProjectionItem::Aggregate(..) | ProjectionItem::WindowFunction { .. } => {
                            false
                        }
                        ProjectionItem::Expr { expr, .. } => !scalar_has_aggregate(expr),
                        _ => true,
                    }) =>
            {
                Some(offset.unwrap_or(0).saturating_add(lim))
            }
            _ => None,
        };

        // Whether any FROM/join table is a virtual/catalog table. Column-name
        // validation is skipped for those, since driver introspection relies on
        // leniently selecting catalog columns that may not all be materialized.
        let mut query_has_virtual = false;
        let (tbl_cols, mut col_names, mut stored_rows) = if let Some(cte_out) =
            crate::cte_scope::lookup(&table_name)
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
                    default_expr: None,
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
                query_has_virtual = true;
                let (cols, rows) = self.get_virtual_table(db_name, schema_name, table_only)?;
                let prefix = table_alias.as_deref().unwrap_or(&table_name);
                let col_names: Vec<String> = cols
                    .iter()
                    .map(|c| format!("{}.{}", prefix, c.name))
                    .collect();
                (cols, col_names, rows)
            } else {
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                self.authorize(ctx, Action::Select, ResourceRef::Table(tbl.id))?;

                let prefix = table_alias.as_deref().unwrap_or(&table_name);
                let col_names: Vec<String> = tbl
                    .columns
                    .iter()
                    .map(|c| format!("{}.{}", prefix, c.name))
                    .collect();

                let mut rows = None;
                // Equality on an indexed column uses the index. The session's
                // uncommitted overlay is merged into the result, so the index is
                // usable inside a transaction rather than forcing a full scan.
                if let Some(FilterExpr::Predicate(Predicate {
                    left,
                    op: CompareOp::Eq,
                    right,
                })) = filter.as_ref()
                {
                    let col_name = left.split('.').last().unwrap_or(left);
                    if let Some(col) = tbl.columns.iter().find(|c| c.name == *col_name) {
                        let col_pos = tbl.columns.iter().position(|c| c.id == col.id);
                        for idx in &tbl.indexes {
                            if idx.key_columns.iter().any(|kc| kc.column_id == col.id) {
                                let val = self.eval_operand(&[], &[], &[], right, &col.data_type);
                                if let Ok(indexed_rows) =
                                    self.index_scan(idx.id, &val, tbl.id, &ctx.session_id)
                                {
                                    rows = Some(self.merge_overlay_eq(
                                        indexed_rows,
                                        tbl.id,
                                        &Self::pk_positions(&tbl),
                                        col_pos,
                                        &val,
                                        &ctx.session_id,
                                    ));
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
                            let out = crate::cte_scope::isolated(|| {
                                self.execute_logical_inner(ctx, plan)
                            })?;
                            out.rows.iter().map(|r| r.values.clone()).collect()
                        } else if let Some(cap) = scan_cap {
                            // Bounded prefix scan; falls back to a full scan if the
                            // session has pending overlay rows for this table.
                            match self.scan_rows_capped(tbl.id, &ctx.session_id, cap)? {
                                Some(rows) => rows,
                                None => self.scan_rows(tbl.id, &ctx.session_id)?,
                            }
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
            // A LATERAL subquery runs for each left row, with that row's values
            // for its outer references; its rows join that row.
            if let Some(plan) = &join.lateral {
                let prefix = join
                    .table_alias
                    .clone()
                    .unwrap_or_else(|| join.table_name.clone());
                let per_row: Vec<QueryOutput> = stored_rows
                    .iter()
                    .map(|r1| {
                        self.execute_logical_inner(
                            ctx,
                            self.correlate_subplan(plan, r1, &col_names),
                        )
                    })
                    .collect::<Result<_>>()?;
                // The subquery's columns, even when there is no left row.
                let header = match per_row.first() {
                    Some(out) => (out.columns.clone(), out.types.clone()),
                    None => {
                        let nulls = vec![Value::Null; col_names.len()];
                        let out = self.execute_logical_inner(
                            ctx,
                            self.correlate_subplan(plan, &nulls, &col_names),
                        )?;
                        (out.columns, out.types)
                    }
                };
                let now = Utc::now();
                let lateral_cols: Vec<ColumnDescriptor> = header
                    .0
                    .iter()
                    .zip(
                        header
                            .1
                            .iter()
                            .chain(std::iter::repeat(&"VARCHAR".to_string())),
                    )
                    .map(|(name, ty)| ColumnDescriptor {
                        id: nodus_catalog::ColumnId::new(),
                        name: name.clone(),
                        version: 1,
                        created_at: now,
                        updated_at: now,
                        state: DescriptorState::Public,
                        data_type: ty.clone(),
                        nullable: true,
                        default_expr: None,
                    })
                    .collect();
                let mut combined_cols = col_names.clone();
                combined_cols.extend(header.0.iter().map(|n| format!("{prefix}.{n}")));
                let mut combined_desc = joined_columns.clone();
                combined_desc.extend(lateral_cols);
                let width = header.0.len();
                if !query_has_virtual && let Some(condition) = join.condition.as_ref() {
                    let mut refs = Vec::new();
                    crate::filter_eval::filter_column_refs(condition, &mut refs);
                    crate::filter_eval::check_column_refs(refs, &combined_cols)?;
                }
                let keep_unmatched = matches!(join.join_type, JoinType::LeftOuter);
                let mut next_rows = Vec::new();
                for (r1, out) in stored_rows.iter().zip(per_row) {
                    let mut matched = false;
                    for r2 in out.rows {
                        let mut combined = r1.clone();
                        combined.extend(r2.values);
                        let is_match = self
                            .eval_filter(
                                ctx,
                                &combined,
                                &combined_cols,
                                &combined_desc,
                                join.condition.as_ref(),
                            )
                            .unwrap_or(false);
                        if is_match {
                            next_rows.push(combined);
                            matched = true;
                        }
                    }
                    if !matched && keep_unmatched {
                        let mut combined = r1.clone();
                        combined.extend(std::iter::repeat_n(Value::Null, width));
                        next_rows.push(combined);
                    }
                }
                stored_rows = next_rows;
                col_names = combined_cols;
                joined_columns = combined_desc;
                continue;
            }
            // Lateral (or standalone) table function: its rows are produced per
            // driving row, so they can't be materialized once. Evaluate against
            // each left row and append the function's columns. A driving row whose
            // function yields nothing is dropped (cross-join-lateral / comma-join
            // semantics).
            if let Some(spec) = &join.table_fn {
                let prefix = join
                    .table_alias
                    .clone()
                    .or_else(|| spec.alias.clone())
                    .unwrap_or_else(|| spec.name.clone());
                // Headers are row-independent; derive them once.
                let (hdr_names, hdr_types, _) = self.eval_table_function(spec, &[], &col_names)?;
                let now = Utc::now();
                let fn_cols: Vec<ColumnDescriptor> = hdr_names
                    .iter()
                    .zip(&hdr_types)
                    .map(|(name, ty)| ColumnDescriptor {
                        id: nodus_catalog::ColumnId::new(),
                        name: name.clone(),
                        version: 1,
                        created_at: now,
                        updated_at: now,
                        state: DescriptorState::Public,
                        data_type: ty.clone(),
                        nullable: true,
                        default_expr: None,
                    })
                    .collect();
                let mut combined_cols = col_names.clone();
                combined_cols.extend(hdr_names.iter().map(|n| format!("{prefix}.{n}")));
                let mut combined_desc = joined_columns.clone();
                combined_desc.extend(fn_cols);

                let keep_empty = matches!(join.join_type, JoinType::LeftOuter);
                let width = hdr_names.len();
                let mut next_rows = Vec::new();
                for r1 in &stored_rows {
                    let (_, _, fn_rows) = self.eval_table_function(spec, r1, &col_names)?;
                    if fn_rows.is_empty() && keep_empty {
                        // LEFT JOIN LATERAL: keep the driving row, NULL-filling the
                        // function's columns when it produces nothing.
                        let mut combined = r1.clone();
                        combined.extend(std::iter::repeat_n(Value::Null, width));
                        next_rows.push(combined);
                    }
                    for fr in fn_rows {
                        let mut combined = r1.clone();
                        combined.extend(fr);
                        next_rows.push(combined);
                    }
                }
                stored_rows = next_rows;
                col_names = combined_cols;
                joined_columns = combined_desc;
                continue;
            }

            let (j_cols, j_rows) = if let Some(cte_out) = crate::cte_scope::lookup(&join.table_name)
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
                        default_expr: None,
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
                    query_has_virtual = true;
                    let (cols, rows) = self.get_virtual_table(j_db, j_sch, j_tbl_name)?;
                    (cols, rows)
                } else {
                    let j_tbl = self.catalog_reader.get_table(j_db, j_sch, j_tbl_name)?;
                    self.authorize(ctx, Action::Select, ResourceRef::Table(j_tbl.id))?;
                    let j_rows = if let Some(vq) = &j_tbl.view_query {
                        let plan: LogicalPlan = serde_json::from_str(vq)?;
                        let out =
                            crate::cte_scope::isolated(|| self.execute_logical_inner(ctx, plan))?;
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

            // `USING (cols)` / `NATURAL` are equi-joins over named columns common
            // to both inputs. Resolve them here, against the actual (prefixed) row
            // schemas, into pairs of combined-row indices to compare for equality
            // — so they compose with chained joins where the left input already
            // spans several tables. `None` means use the `ON` condition instead.
            let named_eq_pairs: Option<Vec<(usize, usize)>> =
                if join.natural || !join.using_columns.is_empty() {
                    let left_len = col_names.len();
                    let unqual = |s: &str| s.rsplit('.').next().unwrap_or(s).to_ascii_lowercase();
                    let names: Vec<String> = if join.natural {
                        col_names
                            .iter()
                            .map(|c| unqual(c))
                            .filter(|n| j_col_names.iter().any(|jc| unqual(jc) == *n))
                            .collect()
                    } else {
                        join.using_columns
                            .iter()
                            .map(|c| c.to_ascii_lowercase())
                            .collect()
                    };
                    let pairs = names
                        .iter()
                        .filter_map(|n| {
                            let li = col_names.iter().position(|c| unqual(c) == *n)?;
                            let ri = j_col_names.iter().position(|c| unqual(c) == *n)?;
                            Some((li, left_len + ri))
                        })
                        .collect();
                    Some(pairs)
                } else {
                    None
                };

            if !query_has_virtual && let Some(condition) = join.condition.as_ref() {
                let mut refs = Vec::new();
                crate::filter_eval::filter_column_refs(condition, &mut refs);
                crate::filter_eval::check_column_refs(refs, &combined_cols)?;
            }
            let mut next_rows = Vec::new();
            let mut right_matched = vec![false; j_rows.len()];
            for r1 in &stored_rows {
                let mut matched = false;
                for (j_idx, r2) in j_rows.iter().enumerate() {
                    let mut combined_row = r1.clone();
                    combined_row.extend(r2.clone());
                    let is_match = match &named_eq_pairs {
                        Some(pairs) => pairs.iter().all(|(l, r)| {
                            crate::value::values_equal(&combined_row[*l], &combined_row[*r])
                        }),
                        None => self
                            .eval_filter(
                                ctx,
                                &combined_row,
                                &combined_cols,
                                &combined_desc,
                                join.condition.as_ref(),
                            )
                            .unwrap_or(false),
                    };
                    if is_match {
                        next_rows.push(combined_row);
                        matched = true;
                        right_matched[j_idx] = true;
                    }
                }
                if !matched && matches!(join.join_type, JoinType::LeftOuter | JoinType::FullOuter) {
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

        // Reject a bare reference to a non-existent column (rather than silently
        // projecting NULL), validated against the full base+join column set.
        // Skipped when a virtual/catalog table is involved — driver introspection
        // relies on leniently selecting catalog columns — and only bare column
        // refs are checked, so computed expressions stay lenient.
        if !query_has_virtual {
            for item in &projection {
                // A function the library lacks is planned as a legacy item; it
                // would otherwise evaluate to NULL.
                if let ProjectionItem::ScalarFunction { func_name, .. } = item {
                    let name = func_name.strip_prefix("PG_CATALOG.").unwrap_or(func_name);
                    if !crate::functions::is_known(name) {
                        anyhow::bail!("function {}() does not exist", name.to_ascii_lowercase());
                    }
                }
            }
            // Every column the query names must be one of its relations'
            // columns; a qualified name must name one of its relations.
            let mut refs = Vec::new();
            for item in &projection {
                match item {
                    ProjectionItem::Column(c) | ProjectionItem::AliasedColumn(c, _) => {
                        refs.push(c.clone())
                    }
                    ProjectionItem::Expr { expr, .. } => {
                        crate::filter_eval::scalar_column_refs(expr, &mut refs)
                    }
                    _ => {}
                }
            }
            if let Some(f) = filter.as_ref() {
                crate::filter_eval::filter_column_refs(f, &mut refs);
            }
            for (_, expr) in &group_exprs {
                crate::filter_eval::scalar_column_refs(expr, &mut refs);
            }
            for target in &key_targets {
                match target {
                    SortTarget::Expr(e) => crate::filter_eval::scalar_column_refs(e, &mut refs),
                    // A bare name may name an output column instead.
                    SortTarget::Name(n) if n.contains('.') => refs.push(n.clone()),
                    _ => {}
                }
            }
            crate::filter_eval::check_column_refs(refs, &col_names)?;
        }

        // `LIMIT 0` returns no rows, so no row is evaluated (as in
        // PostgreSQL, where the limit never pulls from its input). Describe
        // probes rely on this to learn the result shape without running the
        // statement's expressions.
        if limit == Some(0) {
            stored_rows.clear();
        }

        // WHERE: conjunction of typed predicates.
        stored_rows.retain(|r| {
            self.eval_filter(ctx, r, &col_names, &joined_columns, filter.as_ref())
                .unwrap_or(false)
        });

        // Read as the relations of a data-modifying statement: the joined
        // rows, under their qualified column names.
        if raw {
            return Ok(QueryOutput {
                types: joined_columns.iter().map(|c| c.data_type.clone()).collect(),
                columns: col_names,
                rows: stored_rows
                    .into_iter()
                    .map(|values| Row { values })
                    .collect(),
                tag: String::new(),
            });
        }

        // Grouping expressions become columns of each input row. A name that
        // is already an input column keeps that column.
        for (name, expr) in &group_exprs {
            if crate::filter_eval::col_pos(&col_names, name).is_some() {
                continue;
            }
            for row in stored_rows.iter_mut() {
                let value = self.eval_expr(ctx, expr, row, &col_names);
                row.push(value);
            }
            col_names.push(name.clone());
        }
        if !query_has_virtual
            && let Some(missing) = group_by
                .iter()
                .find(|g| crate::filter_eval::col_pos(&col_names, g).is_none())
        {
            anyhow::bail!("column \"{missing}\" does not exist");
        }

        // GROUP BY & Aggregation. A HAVING clause forces the grouping path even
        // without GROUP BY or aggregates: per SQL it treats the whole input as
        // one group (`SELECT 1 FROM t HAVING true` yields at most one row).
        let is_agg = !group_by.is_empty()
            || having.is_some()
            || projection.iter().any(|p| match p {
                ProjectionItem::Aggregate(_, _) => true,
                // An expression like `sum(a) + 1` also forces the grouping path.
                ProjectionItem::Expr { expr, .. } => scalar_has_aggregate(expr),
                _ => false,
            })
            || key_targets
                .iter()
                .any(|t| matches!(t, SortTarget::Expr(e) if scalar_has_aggregate(e)));

        let mut out_rows = Vec::new();
        let mut out_cols = Vec::new();
        // Each output row's source row (for a group, its first row), so ORDER
        // BY can sort by a column that isn't in the projection (e.g. `SELECT
        // count(*) .. GROUP BY b ORDER BY b DESC`).
        let mut out_reps: Vec<Vec<Value>> = Vec::new();
        // Each output row's values of the expression sort keys (NULL for the
        // other keys), computed over its source row or group.
        let mut out_sort_values: Vec<Vec<Value>> = Vec::new();

        if is_agg {
            // The grouping sets to bucket by. Without ROLLUP/CUBE/GROUPING SETS
            // this is a single set equal to the GROUP BY columns.
            let sets: Vec<Vec<String>> = match &grouping_sets {
                Some(s) if !s.is_empty() => s.clone(),
                _ => vec![group_by.clone()],
            };

            for set in &sets {
                // col_pos also resolves qualified refs (`t.col`) against bare
                // column names.
                let set_indices: Vec<Option<usize>> = set
                    .iter()
                    .map(|c| crate::filter_eval::col_pos(&col_names, c))
                    .collect();

                let mut groups: std::collections::BTreeMap<Vec<Vec<u8>>, Vec<Vec<Value>>> =
                    std::collections::BTreeMap::new();

                if stored_rows.is_empty() && set.is_empty() {
                    // Empty set but scalar agg like COUNT(*), yields one row.
                    groups.insert(vec![], vec![]);
                } else {
                    for r in &stored_rows {
                        let key = set_indices
                            .iter()
                            .map(|i| {
                                let val = i.and_then(|idx| r.get(idx)).unwrap_or(&Value::Null);
                                serde_json::to_vec(&crate::value::key_form(val)).unwrap_or_default()
                            })
                            .collect::<Vec<_>>();
                        groups.entry(key).or_default().push(r.clone());
                    }
                }

                for (_k, group_rows) in groups {
                    // HAVING filters whole groups after aggregation.
                    if let Some(h) = having.as_ref() {
                        // A subquery in HAVING reads the group's first row.
                        let rep = group_rows.first().map(Vec::as_slice).unwrap_or(&[]);
                        let resolved = match h {
                            FilterExpr::Scalar(e) if crate::subqueries::contains_subquery(e) => {
                                FilterExpr::Scalar(self.resolve_subqueries(ctx, e, rep, &col_names))
                            }
                            other => other.clone(),
                        };
                        if !eval_having(&resolved, &group_rows, &col_names) {
                            continue;
                        }
                    }
                    let mut out_row = Vec::new();
                    for proj_item in &projection {
                        match proj_item {
                            ProjectionItem::Literal(v) | ProjectionItem::AliasedLiteral(v, _) => {
                                out_row.push(v.clone());
                            }
                            ProjectionItem::Column(c) | ProjectionItem::AliasedColumn(c, _) => {
                                // A grouping column not present in this set is
                                // rolled up to NULL (subtotal / grand-total row).
                                let is_grouping = group_by.iter().any(|g| g == c);
                                let is_active = set.iter().any(|s| s == c);
                                if is_grouping && !is_active {
                                    out_row.push(crate::Value::Null);
                                } else {
                                    let idx = col_names
                                        .iter()
                                        .position(|tc| tc == c || tc.ends_with(&format!(".{}", c)));
                                    out_row.push(
                                        group_rows
                                            .first()
                                            .and_then(|r| idx.and_then(|i| r.get(i)))
                                            .map(|v| v.clone())
                                            .unwrap_or(crate::Value::Null),
                                    );
                                }
                            }
                            ProjectionItem::WindowFunction { .. }
                            | ProjectionItem::ScalarFunction { .. }
                            | ProjectionItem::JsonAccess { .. }
                            | ProjectionItem::CaseWhenEq { .. }
                            | ProjectionItem::Case { .. } => {
                                out_row.push(Value::Null); // MVP fallback
                            }
                            ProjectionItem::Aggregate(op, inner) => {
                                out_row.push(compute_aggregate(op, inner, &group_rows, &col_names));
                            }
                            ProjectionItem::Subquery { plan, .. } => {
                                // Outer references read the group's first row.
                                let rep = group_rows.first().map(Vec::as_slice).unwrap_or(&[]);
                                out_row.push(
                                    self.correlated_scalar_subquery(ctx, plan, rep, &col_names),
                                );
                            }
                            ProjectionItem::Expr { expr, .. } => {
                                // Group-aware eval: aggregates compute over the group,
                                // plain columns read the group's first row.
                                out_row.push(self.eval_grouped(ctx, expr, &group_rows, &col_names));
                            }
                        }
                    }
                    out_rows.push(out_row);
                    out_sort_values.push(
                        key_targets
                            .iter()
                            .map(|target| match target {
                                SortTarget::Expr(e) => {
                                    self.eval_grouped(ctx, e, &group_rows, &col_names)
                                }
                                _ => Value::Null,
                            })
                            .collect(),
                    );
                    out_reps.push(group_rows.first().cloned().unwrap_or_default());
                }
            }

            out_cols = if projection.is_empty() {
                col_names.clone()
            } else {
                projection
                    .iter()
                    .map(|p| match p {
                        ProjectionItem::Column(c) => c.split('.').last().unwrap_or(c).to_string(),
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
                        ProjectionItem::Case { alias, .. } => {
                            alias.clone().unwrap_or_else(|| "case".to_string())
                        }
                        ProjectionItem::Aggregate(op, _) => op.sql_name().to_string(),
                        ProjectionItem::Expr { alias, .. }
                        | ProjectionItem::Subquery { alias, .. } => {
                            alias.clone().unwrap_or_else(|| "?column?".to_string())
                        }
                    })
                    .collect()
            };
        } else {
            out_cols = if projection.is_empty() {
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
                        } => Some(
                            alias
                                .clone()
                                .unwrap_or_else(|| format!("{}{}{}", left, operator, right)),
                        ),
                        ProjectionItem::CaseWhenEq {
                            else_column, alias, ..
                        } => Some(alias.clone().unwrap_or_else(|| {
                            else_column
                                .split('.')
                                .last()
                                .unwrap_or(else_column)
                                .to_string()
                        })),
                        // Searched CASE must yield an out-column, or the row
                        // description and data rows disagree on field count and
                        // the client fails to parse the response.
                        ProjectionItem::Case { alias, .. } => {
                            Some(alias.clone().unwrap_or_else(|| "case".to_string()))
                        }
                        ProjectionItem::Expr { alias, .. }
                        | ProjectionItem::Subquery { alias, .. } => {
                            Some(alias.clone().unwrap_or_else(|| "?column?".to_string()))
                        }
                        _ => None,
                    })
                    .collect()
            };

            // Evaluate Window Functions and Scalar Expressions before projecting
            for (proj_idx, proj_item) in projection.iter().enumerate() {
                match proj_item {
                    ProjectionItem::WindowFunction {
                        func_name,
                        args,
                        partition_by,
                        order_by: w_order_by,
                        alias,
                        frame,
                    } => {
                        let p_indices: Vec<usize> = partition_by
                            .iter()
                            .filter_map(|c| {
                                col_names
                                    .iter()
                                    .position(|tc| tc == c || tc.ends_with(&format!(".{}", c)))
                            })
                            .collect();

                        let o_indices: Vec<(usize, bool)> = w_order_by
                            .iter()
                            .filter_map(|(c, asc)| {
                                col_names
                                    .iter()
                                    .position(|tc| tc == c || tc.ends_with(&format!(".{}", c)))
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
                                let cmp = order_cmp(
                                    a.get(o_idx).unwrap_or(&Value::Null),
                                    b.get(o_idx).unwrap_or(&Value::Null),
                                    asc,
                                    None,
                                );
                                if cmp != std::cmp::Ordering::Equal {
                                    return cmp;
                                }
                            }
                            std::cmp::Ordering::Equal
                        });

                        let mut results = vec![Value::Null; stored_rows.len()];
                        let partition_key_of = |row: &[Value]| -> Vec<Value> {
                            p_indices
                                .iter()
                                .map(|&idx| row.get(idx).unwrap_or(&Value::Null).clone())
                                .collect()
                        };
                        let order_key_of = |row: &[Value]| -> Vec<Value> {
                            o_indices
                                .iter()
                                .map(|&(idx, _)| row.get(idx).unwrap_or(&Value::Null).clone())
                                .collect()
                        };
                        // Without a frame clause, an ordered window ends at the
                        // current row's last peer (PostgreSQL's default `RANGE
                        // UNBOUNDED PRECEDING`); an unordered one is the whole
                        // partition.
                        let default_frame = crate::plan_types::WindowFrame {
                            units: crate::plan_types::WindowFrameUnits::Range,
                            start: crate::plan_types::WindowBound::UnboundedPreceding,
                            end: crate::plan_types::WindowBound::CurrentRow,
                        };
                        let frame = frame
                            .as_ref()
                            .or((!w_order_by.is_empty()).then_some(&default_frame));
                        // RANGE frames only support unbounded/current-row bounds;
                        // a numeric offset needs value arithmetic on the order key.
                        if let Some(f) = frame {
                            use crate::plan_types::{WindowBound as B, WindowFrameUnits as U};
                            if f.units == U::Range
                                && (matches!(f.start, B::Preceding(_) | B::Following(_))
                                    || matches!(f.end, B::Preceding(_) | B::Following(_)))
                            {
                                anyhow::bail!(
                                    "RANGE frame with a numeric offset is not supported; use ROWS"
                                );
                            }
                        }
                        if func_name == "ROW_NUMBER" {
                            let mut current_partition: Vec<Value> = Vec::new();
                            let mut row_num = 1i64;
                            let mut first = true;
                            for &row_idx in &row_indices {
                                let partition_key = partition_key_of(&stored_rows[row_idx]);
                                if first || partition_key != current_partition {
                                    current_partition = partition_key;
                                    row_num = 1;
                                    first = false;
                                }
                                results[row_idx] = Value::Int(row_num);
                                row_num += 1;
                            }
                        } else if func_name == "RANK" || func_name == "DENSE_RANK" {
                            // RANK leaves gaps after ties (1,1,3); DENSE_RANK does not (1,1,2).
                            let dense = func_name == "DENSE_RANK";
                            let mut current_partition: Vec<Value> = Vec::new();
                            let mut rank = 0i64;
                            let mut seen = 0i64;
                            let mut prev_order: Option<Vec<Value>> = None;
                            let mut first = true;
                            for &row_idx in &row_indices {
                                let row = &stored_rows[row_idx];
                                let partition_key = partition_key_of(row);
                                let order_key = order_key_of(row);
                                if first || partition_key != current_partition {
                                    current_partition = partition_key;
                                    rank = 1;
                                    seen = 1;
                                    prev_order = Some(order_key);
                                    first = false;
                                } else {
                                    seen += 1;
                                    if Some(&order_key) != prev_order.as_ref() {
                                        rank = if dense { rank + 1 } else { seen };
                                        prev_order = Some(order_key);
                                    }
                                }
                                results[row_idx] = Value::Int(rank);
                            }
                        } else if func_name == "LAG" || func_name == "LEAD" {
                            // Group the partition-then-order-sorted rows by partition.
                            let groups =
                                partition_groups(&row_indices, &partition_key_of, &stored_rows);
                            let arg_idx = args.first().and_then(|c| {
                                col_names
                                    .iter()
                                    .position(|tc| tc == c || tc.ends_with(&format!(".{}", c)))
                            });
                            let offset: usize =
                                args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);
                            let lead = func_name == "LEAD";
                            for group in &groups {
                                for (pos, &row_idx) in group.iter().enumerate() {
                                    let target = if lead {
                                        pos.checked_add(offset)
                                    } else {
                                        pos.checked_sub(offset)
                                    };
                                    results[row_idx] = target
                                        .and_then(|t| group.get(t))
                                        .and_then(|&ti| {
                                            arg_idx.and_then(|ai| stored_rows[ti].get(ai))
                                        })
                                        .cloned()
                                        .unwrap_or(Value::Null);
                                }
                            }
                        } else if func_name == "NTILE" {
                            // Distribute each ordered partition into `n` buckets
                            // as evenly as possible; the first `len % n` buckets
                            // get one extra row (standard NTILE semantics).
                            let groups =
                                partition_groups(&row_indices, &partition_key_of, &stored_rows);
                            let n: usize = args
                                .first()
                                .and_then(|s| s.parse().ok())
                                .filter(|&n| n > 0)
                                .unwrap_or(1);
                            for group in &groups {
                                let len = group.len();
                                let base = len / n;
                                let rem = len % n;
                                let mut pos = 0usize;
                                for bucket in 1..=n {
                                    let take = base + if bucket <= rem { 1 } else { 0 };
                                    for _ in 0..take {
                                        if let Some(&row_idx) = group.get(pos) {
                                            results[row_idx] = Value::Int(bucket as i64);
                                        }
                                        pos += 1;
                                    }
                                }
                            }
                        } else if func_name == "FIRST_VALUE" || func_name == "LAST_VALUE" {
                            // The argument's value from the first/last row of the
                            // frame (or the whole ordered partition when there is
                            // no explicit frame).
                            let last = func_name == "LAST_VALUE";
                            let groups =
                                partition_groups(&row_indices, &partition_key_of, &stored_rows);
                            let arg_idx = args.first().and_then(|c| {
                                col_names
                                    .iter()
                                    .position(|tc| tc == c || tc.ends_with(&format!(".{}", c)))
                            });
                            for group in &groups {
                                let okeys: Vec<Vec<Value>> = group
                                    .iter()
                                    .map(|&i| order_key_of(&stored_rows[i]))
                                    .collect();
                                for (pos, &row_idx) in group.iter().enumerate() {
                                    // Frame slice for this row (whole group if none).
                                    let (s, e) = match frame {
                                        Some(f) => {
                                            match frame_bounds(f, group.len(), pos, &okeys) {
                                                Some(b) => b,
                                                None => {
                                                    results[row_idx] = Value::Null;
                                                    continue;
                                                }
                                            }
                                        }
                                        None => (0, group.len() - 1),
                                    };
                                    let pick = if last { e } else { s };
                                    results[row_idx] = group
                                        .get(pick)
                                        .and_then(|&gi| {
                                            arg_idx.and_then(|ai| stored_rows[gi].get(ai))
                                        })
                                        .cloned()
                                        .unwrap_or(Value::Null);
                                }
                            }
                        } else if crate::planner::aggregate_op(func_name).is_some() {
                            let groups =
                                partition_groups(&row_indices, &partition_key_of, &stored_rows);
                            let arg = args.first().cloned().unwrap_or_else(|| "*".to_string());
                            for group in &groups {
                                let grows: Vec<Vec<Value>> =
                                    group.iter().map(|&i| stored_rows[i].clone()).collect();
                                if let Some(f) = frame {
                                    // Per-row aggregate over the row's frame slice.
                                    let okeys: Vec<Vec<Value>> = group
                                        .iter()
                                        .map(|&i| order_key_of(&stored_rows[i]))
                                        .collect();
                                    for (pos, &row_idx) in group.iter().enumerate() {
                                        let slice = match frame_bounds(f, group.len(), pos, &okeys)
                                        {
                                            Some((s, e)) => &grows[s..=e],
                                            None => &[],
                                        };
                                        results[row_idx] = window_aggregate(
                                            func_name,
                                            &arg,
                                            args.get(1..).unwrap_or(&[]),
                                            slice,
                                            &col_names,
                                        );
                                    }
                                } else {
                                    // No frame and no ORDER BY: the whole partition.
                                    let agg = window_aggregate(
                                        func_name,
                                        &arg,
                                        args.get(1..).unwrap_or(&[]),
                                        &grows,
                                        &col_names,
                                    );
                                    for &row_idx in group {
                                        results[row_idx] = agg.clone();
                                    }
                                }
                            }
                        } else {
                            anyhow::bail!("Unsupported window function: {}", func_name);
                        }

                        // Append the result to `stored_rows`
                        for (row_idx, row) in stored_rows.iter_mut().enumerate() {
                            row.push(results[row_idx].clone());
                        }
                        col_names.push(format!("__expr_{proj_idx}"));
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
                        col_names.push(format!("__expr_{proj_idx}"));
                    }
                    ProjectionItem::JsonAccess {
                        left,
                        operator,
                        right,
                        alias,
                    } => {
                        let mut results = vec![Value::Null; stored_rows.len()];
                        for (row_idx, row) in stored_rows.iter().enumerate() {
                            let c_idx = col_names
                                .iter()
                                .position(|tc| tc == left || tc.ends_with(&format!(".{}", left)));
                            if let Some(i) = c_idx {
                                if let Some(v) = row.get(i) {
                                    if operator == "->>" {
                                        let json_str = match v {
                                            Value::Jsonb(j) => j.to_string(),
                                            Value::Text(s) => s.clone(),
                                            _ => "".to_string(),
                                        };
                                        if let Ok(json) =
                                            serde_json::from_str::<serde_json::Value>(&json_str)
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
                        col_names.push(format!("__expr_{proj_idx}"));
                    }
                    ProjectionItem::Expr { expr, .. } => {
                        // Compute the expression per row and append it as a
                        // virtual column keyed by projection position, which the
                        // `indices` step resolves back by name below.
                        let vals: Vec<Value> = stored_rows
                            .iter()
                            .map(|row| self.eval_expr(ctx, expr, row, &col_names))
                            .collect();
                        for (row, v) in stored_rows.iter_mut().zip(vals) {
                            row.push(v);
                        }
                        col_names.push(format!("__expr_{proj_idx}"));
                    }
                    ProjectionItem::Subquery { plan, .. } => {
                        let vals: Vec<Value> = stored_rows
                            .iter()
                            .map(|row| self.correlated_scalar_subquery(ctx, plan, row, &col_names))
                            .collect();
                        for (row, v) in stored_rows.iter_mut().zip(vals) {
                            row.push(v);
                        }
                        col_names.push(format!("__expr_{proj_idx}"));
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
                            ProjectionItem::Literal(_) | ProjectionItem::AliasedLiteral(_, _) => {
                                "".to_string()
                            }
                            // Computed items are stored under their position, so
                            // two items with the same output name (or one named
                            // like an input column) stay distinct.
                            ProjectionItem::WindowFunction { .. }
                            | ProjectionItem::ScalarFunction { .. }
                            | ProjectionItem::JsonAccess { .. } => format!("__expr_{pi}"),
                            ProjectionItem::CaseWhenEq {
                                else_column, alias, ..
                            } => alias.clone().unwrap_or_else(|| else_column.clone()),
                            ProjectionItem::Expr { .. } | ProjectionItem::Subquery { .. } => {
                                format!("__expr_{}", pi)
                            }
                            _ => c.clone(),
                        };
                        col_names.iter().position(|tc| {
                            tc == &actual_col || tc.ends_with(&format!(".{}", actual_col))
                        })
                    }
                })
                .collect();

            if !key_targets.is_empty() {
                out_sort_values = stored_rows
                    .iter()
                    .map(|row| {
                        key_targets
                            .iter()
                            .map(|target| match target {
                                SortTarget::Expr(e) => self.eval_expr(ctx, e, row, &col_names),
                                _ => Value::Null,
                            })
                            .collect()
                    })
                    .collect();
            }
            out_rows = stored_rows
                .iter()
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
                                    let left_value =
                                        left_idx.and_then(|idx| r.get(idx)).unwrap_or(&Value::Null);
                                    if compare(left_value, equals) == std::cmp::Ordering::Equal {
                                        if let Some(then_column) = then_column {
                                            col_names
                                                .iter()
                                                .position(|tc| {
                                                    tc == then_column
                                                        || tc
                                                            .ends_with(&format!(".{}", then_column))
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
                                ProjectionItem::Case {
                                    branches,
                                    else_result,
                                    ..
                                } => {
                                    let matched = branches.iter().find_map(|(pred, then)| {
                                        let hit = self.eval_filter(
                                            ctx,
                                            &r,
                                            &col_names,
                                            &joined_columns,
                                            Some(&FilterExpr::Predicate(pred.clone())),
                                        ) == Some(true);
                                        hit.then(|| {
                                            self.eval_operand(
                                                &r,
                                                &col_names,
                                                &joined_columns,
                                                then,
                                                "VARCHAR",
                                            )
                                        })
                                    });
                                    matched
                                        .or_else(|| {
                                            else_result.as_ref().map(|o| {
                                                self.eval_operand(
                                                    &r,
                                                    &col_names,
                                                    &joined_columns,
                                                    o,
                                                    "VARCHAR",
                                                )
                                            })
                                        })
                                        .unwrap_or(Value::Null)
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
            if !key_targets.is_empty() {
                out_reps = stored_rows;
            }
        }

        // ORDER BY and DISTINCT ON, over the output rows. A name means an
        // output column before an input column (a qualified name only an input
        // column); an input column reads the row's source row, or its group's
        // first row.
        if !key_targets.is_empty() {
            enum KeySource {
                Output(usize),
                Source(usize),
                /// The key's own computed value, by key position.
                Computed(usize),
                /// An unresolved key of a catalog query: NULL for every row.
                Missing,
            }
            let mut sources = Vec::with_capacity(key_targets.len());
            for (key_index, target) in key_targets.iter().enumerate() {
                let source = match target {
                    SortTarget::Output(i) if *i < out_cols.len() => KeySource::Output(*i),
                    SortTarget::Output(i) => {
                        anyhow::bail!("ORDER BY position {} is not in select list", i + 1)
                    }
                    SortTarget::Name(name) => {
                        let output = if name.contains('.') {
                            None
                        } else {
                            out_cols.iter().position(|c| c == name)
                        };
                        match output {
                            Some(i) => KeySource::Output(i),
                            None => match crate::filter_eval::col_pos(&col_names, name) {
                                Some(i) => KeySource::Source(i),
                                // Catalog queries resolve columns leniently.
                                None if query_has_virtual => KeySource::Missing,
                                None => anyhow::bail!("column \"{name}\" does not exist"),
                            },
                        }
                    }
                    SortTarget::Expr(_) => KeySource::Computed(key_index),
                };
                sources.push(source);
            }
            let keys: Vec<Vec<Value>> = (0..out_rows.len())
                .map(|row| {
                    sources
                        .iter()
                        .map(|source| {
                            let cell = match source {
                                KeySource::Output(i) => out_rows[row].get(*i),
                                KeySource::Source(i) => out_reps.get(row).and_then(|r| r.get(*i)),
                                KeySource::Computed(k) => {
                                    out_sort_values.get(row).and_then(|values| values.get(*k))
                                }
                                KeySource::Missing => None,
                            };
                            cell.cloned().unwrap_or(Value::Null)
                        })
                        .collect()
                })
                .collect();
            let mut perm: Vec<usize> = (0..out_rows.len()).collect();
            perm.sort_by(|&x, &y| {
                for (i, key) in sort.iter().enumerate() {
                    let ord = order_cmp(&keys[x][i], &keys[y][i], key.ascending, key.nulls_first);
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
            // DISTINCT ON keeps the first row (in sort order) of each key.
            if !distinct_on.is_empty() {
                let mut seen = std::collections::HashSet::new();
                perm.retain(|&row| {
                    let key: Vec<Vec<u8>> = keys[row][sort.len()..]
                        .iter()
                        .map(|v| serde_json::to_vec(&crate::value::key_form(v)).unwrap_or_default())
                        .collect();
                    seen.insert(key)
                });
            }
            let mut rows: Vec<Option<Vec<Value>>> = out_rows.into_iter().map(Some).collect();
            out_rows = perm.iter().filter_map(|&i| rows[i].take()).collect();
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
                ProjectionItem::Column(col) | ProjectionItem::AliasedColumn(col, _) => Some(col),
                _ => None,
            }) {
                if let Some(source_idx) = col_names.iter().position(|candidate| {
                    candidate == source_col || candidate.ends_with(&format!(".{}", source_col))
                }) {
                    if let Some(col_desc) = joined_columns.get(source_idx) {
                        ty = col_desc.data_type.clone();
                    }
                }
            }

            if let Some(inferred) = projection.get(i).and_then(|item| {
                crate::result_types::projection_type(item, |source| {
                    col_names
                        .iter()
                        .position(|name| name == source || name.ends_with(&format!(".{source}")))
                        .and_then(|index| joined_columns.get(index))
                        .map(|column| column.data_type.clone())
                })
            }) {
                types.push(inferred);
                continue;
            }
            if ty == "VARCHAR" && !rows.is_empty() {
                if let Some(val) = rows[0].values.get(i) {
                    match val {
                        Value::Int(_) => ty = "INTEGER".to_string(),
                        Value::Float(_) => ty = "DOUBLE".to_string(),
                        Value::Numeric(_) => ty = "NUMERIC".to_string(),
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
}

/// Compares two cells for an ORDER BY key, honouring the ascending flag and an
/// optional explicit `NULLS FIRST`/`NULLS LAST` override. With no override the
/// default matches PostgreSQL: NULLs sort last on ASC and first on DESC.
pub(crate) fn order_cmp(
    a: &Value,
    b: &Value,
    asc: bool,
    nulls_first: Option<bool>,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let a_null = a == &Value::Null;
    let b_null = b == &Value::Null;
    match (a_null, b_null) {
        (true, true) => Ordering::Equal,
        (true, false) | (false, true) => {
            // PostgreSQL's default treats NULL as larger than every value:
            // last when ascending, first when descending.
            let nf = nulls_first.unwrap_or(!asc);
            // "Nulls first" means the NULL side is the lesser (earlier) one.
            if a_null == nf {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (false, false) => {
            let ord = compare(a, b);
            if asc { ord } else { ord.reverse() }
        }
    }
}

/// Computes the inclusive `[start, end]` index range within an ordered
/// partition group that a window frame covers for the row at `pos`. Returns
/// `None` when the frame is empty for that row. `order_keys` holds each group
/// position's ORDER BY key (used for peer detection in `RANGE` frames).
fn frame_bounds(
    frame: &crate::plan_types::WindowFrame,
    group_len: usize,
    pos: usize,
    order_keys: &[Vec<Value>],
) -> Option<(usize, usize)> {
    use crate::plan_types::{WindowBound as B, WindowFrameUnits as U};
    if group_len == 0 {
        return None;
    }
    let last = (group_len - 1) as i64;
    let p = pos as i64;
    let (start, end): (i64, i64) = match frame.units {
        U::Rows => {
            let start = match &frame.start {
                B::UnboundedPreceding => 0,
                B::Preceding(k) => p - *k,
                B::CurrentRow => p,
                B::Following(k) => p + *k,
                B::UnboundedFollowing => group_len as i64, // empty
            };
            let end = match &frame.end {
                B::UnboundedPreceding => -1, // empty
                B::Preceding(k) => p - *k,
                B::CurrentRow => p,
                B::Following(k) => p + *k,
                B::UnboundedFollowing => last,
            };
            (start, end)
        }
        U::Range => {
            // Only unbounded / current-row bounds reach here (numeric offsets
            // are rejected earlier). CURRENT ROW spans the row's ORDER BY peers.
            let peer_start = (0..=pos)
                .find(|&i| order_keys[i] == order_keys[pos])
                .unwrap_or(pos) as i64;
            let peer_end = (pos..group_len)
                .rev()
                .find(|&i| order_keys[i] == order_keys[pos])
                .unwrap_or(pos) as i64;
            let start = match &frame.start {
                B::UnboundedPreceding => 0,
                B::CurrentRow => peer_start,
                B::UnboundedFollowing => group_len as i64,
                _ => 0,
            };
            let end = match &frame.end {
                B::UnboundedPreceding => -1,
                B::CurrentRow => peer_end,
                B::UnboundedFollowing => last,
                _ => last,
            };
            (start, end)
        }
    };
    let start = start.max(0);
    let end = end.min(last);
    if start > end || start > last || end < 0 {
        None
    } else {
        Some((start as usize, end as usize))
    }
}

/// Computes a single windowed aggregate (`SUM`/`COUNT`/`AVG`/`MIN`/`MAX`) over
/// the given rows, matching the grouped-aggregate semantics.
fn window_aggregate(
    func_name: &str,
    arg: &str,
    extra: &[String],
    rows: &[Vec<Value>],
    col_names: &[String],
) -> Value {
    let Some(op) = crate::planner::aggregate_op(func_name) else {
        return Value::Null;
    };
    if arg == "*" {
        return Value::Int(rows.len() as i64);
    }
    // A further argument is a column of the row, or else a constant
    // (`string_agg(s, ',')`).
    let resolve = |name: &str, row: &[Value]| match crate::filter_eval::col_pos(col_names, name) {
        Some(i) => row.get(i).cloned().unwrap_or(Value::Null),
        None => Value::Text(name.to_string()),
    };
    let inputs: Vec<(Value, Vec<Value>)> = rows
        .iter()
        .map(|row| {
            let value = crate::filter_eval::col_pos(col_names, arg)
                .and_then(|i| row.get(i))
                .cloned()
                .unwrap_or(Value::Null);
            (value, extra.iter().map(|e| resolve(e, row)).collect())
        })
        .collect();
    crate::aggregates::aggregate_inputs(&op, &inputs)
}

fn partition_groups<F: Fn(&[Value]) -> Vec<Value>>(
    row_indices: &[usize],
    partition_key_of: F,
    stored_rows: &[Vec<Value>],
) -> Vec<Vec<usize>> {
    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut current: Vec<Value> = Vec::new();
    let mut first = true;
    for &row_idx in row_indices {
        let pk = partition_key_of(&stored_rows[row_idx]);
        if first || pk != current {
            groups.push(Vec::new());
            current = pk;
            first = false;
        }
        groups.last_mut().unwrap().push(row_idx);
    }
    groups
}
