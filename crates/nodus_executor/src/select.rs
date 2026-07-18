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

        let mut result: Vec<Vec<Value>> =
            seed_out.rows.into_iter().map(|r| r.values).collect();
        let mut working = result.clone();
        let mut guard = 0u32;
        while !working.is_empty() {
            guard += 1;
            if guard > 10_000 {
                anyhow::bail!("WITH RECURSIVE did not terminate within 10000 iterations");
            }
            // Feed the working table into the recursive term under the CTE name.
            let mut term = recursive_term.clone();
            inject_inline_cte(&mut term, name, columns.clone(), types.clone(), working.clone());
            let iter = self.execute_logical_inner(ctx, term)?;

            let mut new_rows: Vec<Vec<Value>> = Vec::new();
            for r in iter.rows {
                let vals = r.values;
                if all {
                    new_rows.push(vals);
                } else if !result.iter().any(|x| x == &vals)
                    && !new_rows.iter().any(|x| x == &vals)
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
    pub(crate) fn exec_select(
        &self,
        ctx: &ExecutionContext,
        ctes: Vec<(String, Box<LogicalPlan>)>,
        table_name: String,
        table_alias: Option<String>,
        joins: Vec<Join>,
        projection: Vec<ProjectionItem>,
        group_by: Vec<String>,
        filter: Option<FilterExpr>,
        having: Option<FilterExpr>,
        grouping_sets: Option<Vec<Vec<String>>>,
        order_by: Vec<(String, bool, Option<bool>)>,
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: bool,
    ) -> Result<QueryOutput> {
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
            cte_results.insert(name, out);
        }

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
                    && order_by.is_empty()
                    && having.is_none()
                    && filter.is_none()
                    && !distinct
                    && projection.iter().all(|p| {
                        !matches!(
                            p,
                            ProjectionItem::Aggregate(..) | ProjectionItem::WindowFunction { .. }
                        )
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
                            let out = self.execute_logical_inner(ctx, plan)?;
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

            let (j_cols, j_rows) = if let Some(cte_out) = cte_results.get(&join.table_name) {
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
                if let ProjectionItem::Column(c) | ProjectionItem::AliasedColumn(c, _) = item {
                    let bare = c.rsplit('.').next().unwrap_or(c);
                    let known = col_names
                        .iter()
                        .any(|cn| cn == c || cn == bare || cn.ends_with(&format!(".{bare}")));
                    if !known {
                        anyhow::bail!("column \"{bare}\" does not exist");
                    }
                }
            }
        }

        // WHERE: conjunction of typed predicates.
        stored_rows.retain(|r| {
            self.eval_filter(ctx, r, &col_names, &joined_columns, filter.as_ref())
                .unwrap_or(false)
        });

        // GROUP BY & Aggregation
        let is_agg = !group_by.is_empty()
            || projection.iter().any(|p| match p {
                ProjectionItem::Aggregate(_, _) => true,
                // An expression like `sum(a) + 1` also forces the grouping path.
                ProjectionItem::Expr { expr, .. } => scalar_has_aggregate(expr),
                _ => false,
            });

        if !is_agg {
            if !order_by.is_empty() {
                let mut order_indices = Vec::new();
                for (ocol, asc, nf) in &order_by {
                    let idx = col_names
                        .iter()
                        .position(|c| c == ocol || c.ends_with(&format!(".{}", ocol)));
                    if let Some(i) = idx {
                        order_indices.push((i, *asc, *nf));
                    }
                }
                stored_rows.sort_by(|a, b| {
                    for (idx, asc, nf) in &order_indices {
                        let ord = order_cmp(
                            a.get(*idx).unwrap_or(&crate::Value::Null),
                            b.get(*idx).unwrap_or(&crate::Value::Null),
                            *asc,
                            *nf,
                        );
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }

        let mut out_rows = Vec::new();
        let mut out_cols = Vec::new();

        if is_agg {
            // The grouping sets to bucket by. Without ROLLUP/CUBE/GROUPING SETS
            // this is a single set equal to the GROUP BY columns.
            let sets: Vec<Vec<String>> = match &grouping_sets {
                Some(s) if !s.is_empty() => s.clone(),
                _ => vec![group_by.clone()],
            };

            for set in &sets {
                let set_indices: Vec<Option<usize>> = set
                    .iter()
                    .map(|c| {
                        col_names
                            .iter()
                            .position(|tc| tc == c || tc.ends_with(&format!(".{}", c)))
                    })
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
                                serde_json::to_vec(val).unwrap_or_default()
                            })
                            .collect::<Vec<_>>();
                        groups.entry(key).or_default().push(r.clone());
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
                                    let idx = col_names.iter().position(|tc| {
                                        tc == c || tc.ends_with(&format!(".{}", c))
                                    });
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
                            ProjectionItem::Expr { expr, .. } => {
                                // Group-aware eval: aggregates compute over the group,
                                // plain columns read the group's first row.
                                out_row
                                    .push(eval_scalar_expr_grouped(expr, &group_rows, &col_names));
                            }
                        }
                    }
                    out_rows.push(out_row);
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
                        ProjectionItem::Aggregate(op, inner) => {
                            format!("{:?}({})", op, inner)
                        }
                        ProjectionItem::Expr { alias, .. } => {
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
                        ProjectionItem::Expr { alias, .. } => {
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
                                let okeys: Vec<Vec<Value>> =
                                    group.iter().map(|&i| order_key_of(&stored_rows[i])).collect();
                                for (pos, &row_idx) in group.iter().enumerate() {
                                    // Frame slice for this row (whole group if none).
                                    let (s, e) = match frame {
                                        Some(f) => match frame_bounds(f, group.len(), pos, &okeys) {
                                            Some(b) => b,
                                            None => {
                                                results[row_idx] = Value::Null;
                                                continue;
                                            }
                                        },
                                        None => (0, group.len() - 1),
                                    };
                                    let pick = if last { e } else { s };
                                    results[row_idx] = group
                                        .get(pick)
                                        .and_then(|&gi| arg_idx.and_then(|ai| stored_rows[gi].get(ai)))
                                        .cloned()
                                        .unwrap_or(Value::Null);
                                }
                            }
                        } else if matches!(
                            func_name.as_str(),
                            "SUM" | "COUNT" | "AVG" | "MIN" | "MAX"
                        ) {
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
                                        let slice = match frame_bounds(f, group.len(), pos, &okeys) {
                                            Some((s, e)) => &grows[s..=e],
                                            None => &[],
                                        };
                                        results[row_idx] =
                                            window_aggregate(func_name, &arg, slice, &col_names);
                                    }
                                } else {
                                    // No explicit frame: aggregate over the whole partition.
                                    let agg = window_aggregate(func_name, &arg, &grows, &col_names);
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
                        let w_col_name = alias
                            .clone()
                            .unwrap_or_else(|| format!("{}{}{}", left, operator, right));
                        col_names.push(w_col_name);
                    }
                    ProjectionItem::Expr { expr, .. } => {
                        // Compute the expression per row and append it as a
                        // virtual column keyed by projection position, which the
                        // `indices` step resolves back by name below.
                        let vals: Vec<Value> = stored_rows
                            .iter()
                            .map(|row| eval_scalar_expr(expr, row, &col_names))
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
                            } => alias.clone().unwrap_or_else(|| else_column.clone()),
                            ProjectionItem::Expr { .. } => format!("__expr_{}", pi),
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
        }

        // ORDER BY for aggregates (uses out_cols). For non-aggregates, it was already sorted.
        if is_agg {
            if !order_by.is_empty() {
                let mut order_indices = Vec::new();
                for (ocol, asc, nf) in &order_by {
                    let idx = out_cols
                        .iter()
                        .position(|c| c == ocol || c.ends_with(&format!(".{}", ocol)));
                    if let Some(i) = idx {
                        order_indices.push((i, *asc, *nf));
                    }
                }
                out_rows.sort_by(|a, b| {
                    for (idx, asc, nf) in &order_indices {
                        let ord = order_cmp(
                            a.get(*idx).unwrap_or(&crate::Value::Null),
                            b.get(*idx).unwrap_or(&crate::Value::Null),
                            *asc,
                            *nf,
                        );
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
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
}

/// Groups partition-then-order-sorted row indices into per-partition runs,
/// used by LAG/LEAD and aggregate window functions.
/// Injects a pre-computed relation as a CTE named `name` into a Select plan
/// (replacing any existing CTE of that name), so the plan's FROM/join resolves
/// the name to these rows. Used to feed a recursive term its working table.
fn inject_inline_cte(
    plan: &mut LogicalPlan,
    name: &str,
    columns: Vec<String>,
    types: Vec<String>,
    rows: Vec<Vec<Value>>,
) {
    if let LogicalPlan::Select { ctes, .. } = plan {
        ctes.retain(|(n, _)| n != name);
        ctes.insert(
            0,
            (
                name.to_string(),
                Box::new(LogicalPlan::InlineRows {
                    columns,
                    types,
                    rows,
                }),
            ),
        );
    }
}

/// Compares two cells for an ORDER BY key, honouring the ascending flag and an
/// optional explicit `NULLS FIRST`/`NULLS LAST` override. With no override the
/// default matches PostgreSQL: NULLs sort first on ASC and last on DESC.
fn order_cmp(
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
            // Default: nulls first on ASC, last on DESC — i.e. nulls_first == asc.
            let nf = nulls_first.unwrap_or(asc);
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
    rows: &[Vec<Value>],
    col_names: &[String],
) -> Value {
    match func_name {
        "AVG" => {
            let sum = compute_aggregate(&AggregateOp::Sum, arg, rows, col_names);
            let count = rows.len() as f64;
            match sum {
                Value::Int(s) if count > 0.0 => Value::Float(s as f64 / count),
                Value::Float(s) if count > 0.0 => Value::Float(s / count),
                _ => Value::Null,
            }
        }
        "SUM" => compute_aggregate(&AggregateOp::Sum, arg, rows, col_names),
        "COUNT" => compute_aggregate(&AggregateOp::Count, arg, rows, col_names),
        "MIN" => compute_aggregate(&AggregateOp::Min, arg, rows, col_names),
        "MAX" => compute_aggregate(&AggregateOp::Max, arg, rows, col_names),
        _ => Value::Null,
    }
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
