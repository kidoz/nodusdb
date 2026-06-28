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
        order_by: Vec<(String, bool)>,
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
            let out = self.execute_logical_inner(ctx, *cte_plan)?;
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
                    })
                    .collect();
                let mut combined_cols = col_names.clone();
                combined_cols.extend(hdr_names.iter().map(|n| format!("{prefix}.{n}")));
                let mut combined_desc = joined_columns.clone();
                combined_desc.extend(fn_cols);

                let mut next_rows = Vec::new();
                for r1 in &stored_rows {
                    let (_, _, fn_rows) = self.eval_table_function(spec, r1, &col_names)?;
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
                        ProjectionItem::Literal(v) | ProjectionItem::AliasedLiteral(v, _) => {
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
                        | ProjectionItem::CaseWhenEq { .. }
                        | ProjectionItem::Case { .. } => {
                            out_row.push(Value::Null); // MVP fallback
                        }
                        ProjectionItem::Aggregate(op, inner) => {
                            out_row.push(compute_aggregate(op, inner, &group_rows, &col_names));
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
                        _ => None,
                    })
                    .collect()
            };

            // Evaluate Window Functions and Scalar Expressions before projecting
            for proj_item in projection.iter() {
                match proj_item {
                    ProjectionItem::WindowFunction {
                        func_name,
                        args,
                        partition_by,
                        order_by: w_order_by,
                        alias,
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
                        } else if matches!(
                            func_name.as_str(),
                            "SUM" | "COUNT" | "AVG" | "MIN" | "MAX"
                        ) {
                            // Aggregate window over the whole partition (no frame clause).
                            let groups =
                                partition_groups(&row_indices, &partition_key_of, &stored_rows);
                            let arg = args.first().cloned().unwrap_or_else(|| "*".to_string());
                            for group in &groups {
                                let group_rows: Vec<Vec<Value>> =
                                    group.iter().map(|&i| stored_rows[i].clone()).collect();
                                let agg = match func_name.as_str() {
                                    "AVG" => {
                                        let sum = compute_aggregate(
                                            &AggregateOp::Sum,
                                            &arg,
                                            &group_rows,
                                            &col_names,
                                        );
                                        let count = group_rows.len() as f64;
                                        match sum {
                                            Value::Int(s) if count > 0.0 => {
                                                Value::Float(s as f64 / count)
                                            }
                                            Value::Float(s) if count > 0.0 => {
                                                Value::Float(s / count)
                                            }
                                            _ => Value::Null,
                                        }
                                    }
                                    "SUM" => compute_aggregate(
                                        &AggregateOp::Sum,
                                        &arg,
                                        &group_rows,
                                        &col_names,
                                    ),
                                    "COUNT" => compute_aggregate(
                                        &AggregateOp::Count,
                                        &arg,
                                        &group_rows,
                                        &col_names,
                                    ),
                                    "MIN" => compute_aggregate(
                                        &AggregateOp::Min,
                                        &arg,
                                        &group_rows,
                                        &col_names,
                                    ),
                                    "MAX" => compute_aggregate(
                                        &AggregateOp::Max,
                                        &arg,
                                        &group_rows,
                                        &col_names,
                                    ),
                                    _ => Value::Null,
                                };
                                for &row_idx in group {
                                    results[row_idx] = agg.clone();
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
