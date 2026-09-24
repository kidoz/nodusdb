//! Query planning: SELECT/set-op planning and object-name resolution.
use super::*;
use crate::*;
use anyhow::Result;
use nodus_catalog::TableConstraint;

pub(crate) fn table_name_of(relation: &sqlparser::ast::TableFactor) -> Result<String> {
    match relation {
        sqlparser::ast::TableFactor::Table { name, .. } => Ok(name.to_string()),
        other => anyhow::bail!("Unsupported table relation: {:?}", other),
    }
}

pub fn parse_object_name(name: &str) -> Result<(&str, &str, &str)> {
    let parts: Vec<&str> = name.split('.').collect();
    match parts.len() {
        1 => Ok(("default", "public", parts[0].trim_matches('"'))),
        2 => Ok((
            "default",
            parts[0].trim_matches('"'),
            parts[1].trim_matches('"'),
        )),
        3 => Ok((
            parts[0].trim_matches('"'),
            parts[1].trim_matches('"'),
            parts[2].trim_matches('"'),
        )),
        _ => anyhow::bail!("Invalid object name: {}", name),
    }
}

/// Resolves an `ORDER BY` item's direction: `true` for ascending, the default.
/// `USING <operator>` has no executor support, so it is rejected rather than
/// silently sorted ascending.
fn sort_ascending(options: &sqlparser::ast::OrderByOptions) -> Result<bool> {
    match &options.sort {
        None | Some(sqlparser::ast::OrderBySort::Asc) => Ok(true),
        Some(sqlparser::ast::OrderBySort::Desc) => Ok(false),
        Some(sqlparser::ast::OrderBySort::Using(op)) => {
            anyhow::bail!("Unsupported ORDER BY USING operator: {op}")
        }
    }
}

/// Hard ceiling on nested-query planning depth (CTEs, set operations, and
/// subqueries each recurse through [`plan_query`]). sqlparser already caps
/// expression depth at parse time, but nested query structures recurse here too;
/// this guard turns a pathologically nested query into a clean error instead of
/// a stack overflow — which, unlike a panic, cannot be caught and would abort
/// the whole process.
const MAX_QUERY_PLAN_DEPTH: usize = 100;

thread_local! {
    static PLAN_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// RAII guard that increments the per-thread planning depth on entry and
/// decrements it on drop, erroring if the depth ceiling is exceeded. Planning is
/// synchronous and single-threaded per statement (it runs on a blocking-pool
/// thread), so a thread-local counter is sufficient.
struct PlanDepthGuard;

impl PlanDepthGuard {
    fn enter() -> Result<Self> {
        PLAN_DEPTH.with(|d| {
            let next = d.get() + 1;
            if next > MAX_QUERY_PLAN_DEPTH {
                anyhow::bail!("query nesting too deep (limit {MAX_QUERY_PLAN_DEPTH})");
            }
            d.set(next);
            Ok(PlanDepthGuard)
        })
    }
}

impl Drop for PlanDepthGuard {
    fn drop(&mut self) {
        PLAN_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

/// True if any FROM/join relation in `plan` refers to the table `name` (used to
/// detect a recursive CTE's self-reference). A CTE that shadows `name` stops the
/// descent into that sub-plan.
pub(crate) fn plan_references_table(plan: &LogicalPlan, name: &str) -> bool {
    match plan {
        LogicalPlan::Select {
            table_name,
            joins,
            ctes,
            ..
        } => {
            table_name == name
                || joins.iter().any(|j| j.table_name == name)
                || ctes
                    .iter()
                    .any(|(n, p)| n != name && plan_references_table(p, name))
        }
        LogicalPlan::SetOp { left, right, .. } => {
            plan_references_table(left, name) || plan_references_table(right, name)
        }
        LogicalPlan::RecursiveCte {
            seed,
            recursive_term,
            ..
        } => plan_references_table(seed, name) || plan_references_table(recursive_term, name),
        _ => false,
    }
}

/// Resolves a 1-based ordinal (`ORDER BY 1` / `GROUP BY 1`) to the referenced
/// projection item's output column name, mirroring the executor's out-column
/// naming so the sort/group lookup finds it.
fn ordinal_target(projection: &[ProjectionItem], n: usize) -> Option<String> {
    match projection.get(n.checked_sub(1)?)? {
        ProjectionItem::Column(c) => Some(c.clone()),
        ProjectionItem::AliasedColumn(_, a) | ProjectionItem::AliasedLiteral(_, a) => {
            Some(a.clone())
        }
        ProjectionItem::Aggregate(op, inner) => Some(format!("{op:?}({inner})")),
        ProjectionItem::Expr { alias, .. } => alias.clone(),
        ProjectionItem::ScalarFunction {
            func_name, alias, ..
        }
        | ProjectionItem::WindowFunction {
            func_name, alias, ..
        } => Some(alias.clone().unwrap_or_else(|| func_name.clone())),
        _ => None,
    }
}

pub(crate) fn plan_query(query: &sqlparser::ast::Query, params: &[Value]) -> Result<LogicalPlan> {
    use sqlparser::ast::*;

    let _depth_guard = PlanDepthGuard::enter()?;

    let mut ctes = Vec::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            let cte_name = cte.alias.name.value.clone();
            let column_aliases: Vec<String> = cte
                .alias
                .columns
                .iter()
                .map(|c| c.name.value.clone())
                .collect();
            let cte_plan = plan_query(&cte.query, params)?;
            // A `WITH RECURSIVE` CTE whose body is `seed UNION[/ALL] term` where
            // the term self-references the CTE becomes a RecursiveCte so the CTE
            // loop can run the seed-then-iterate fixpoint. Non-self-referencing
            // bodies stay as ordinary plans even under WITH RECURSIVE.
            let cte_plan = match cte_plan {
                LogicalPlan::SetOp {
                    op: SetOpKind::Union,
                    all,
                    left,
                    right,
                } if with.recursive && plan_references_table(&right, &cte_name) => {
                    LogicalPlan::RecursiveCte {
                        all,
                        column_aliases,
                        seed: left,
                        recursive_term: right,
                    }
                }
                other => other,
            };
            ctes.push((cte_name, Box::new(cte_plan)));
        }
    }

    if let SetExpr::SetOperation {
        op,
        set_quantifier,
        left,
        right,
    } = &*query.body
    {
        let kind = match op {
            SetOperator::Union => SetOpKind::Union,
            SetOperator::Intersect => SetOpKind::Intersect,
            // `MINUS` is a non-standard alias for `EXCEPT` (Oracle/ClickHouse).
            SetOperator::Except | SetOperator::Minus => SetOpKind::Except,
        };
        let all = *set_quantifier == SetQuantifier::All;
        let wrap = |body: &Box<SetExpr>| Query {
            with: None,
            body: body.clone(),
            order_by: None,
            limit_clause: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
        };
        let left_plan = plan_query(&wrap(left), params)?;
        let right_plan = plan_query(&wrap(right), params)?;
        return Ok(LogicalPlan::SetOp {
            op: kind,
            all,
            left: Box::new(left_plan),
            right: Box::new(right_plan),
        });
    }

    let SetExpr::Select(select) = &*query.body else {
        anyhow::bail!("Unsupported query body");
    };

    if select.from.is_empty() {
        let mut values = Vec::new();
        let mut deferred = Vec::new();
        for item in &select.projection {
            let (expr, alias) = match item {
                SelectItem::UnnamedExpr(expr) => (expr, default_output_name(expr)),
                SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.to_string()),
                _ => anyhow::bail!("Unsupported scalar select item"),
            };
            // A CAST fixes the column's type even when the value is NULL, so
            // `NULL::int` reports int4 rather than defaulting to text.
            let type_hint = if let Expr::Cast { data_type, .. } = expr {
                Some(data_type.to_string())
            } else {
                None
            };
            let item = match expr {
                Expr::Subquery(query) => {
                    DeferredItem::Subquery(Box::new(plan_query(query, params)?))
                }
                Expr::Exists { subquery, negated } => DeferredItem::Exists {
                    plan: Box::new(plan_query(subquery, params)?),
                    negated: *negated,
                },
                _ if !matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                    && expr_to_value(expr, params).is_some() =>
                {
                    let value = expr_to_value(expr, params).expect("checked above");
                    values.push((alias, value, type_hint));
                    deferred.push(None);
                    continue;
                }
                _ => {
                    let scalar =
                        lower_scalar(expr, params).ok_or_else(|| {
                            anyhow::anyhow!(unknown_function_error(expr).unwrap_or_else(
                                || format!("Unsupported expression in SELECT: {expr}")
                            ))
                        })?;
                    if let Some(column) = first_column_reference(&scalar) {
                        anyhow::bail!("column \"{column}\" does not exist");
                    }
                    DeferredItem::Scalar(scalar)
                }
            };
            values.push((alias, crate::Value::Null, type_hint));
            deferred.push(Some(item));
        }
        return Ok(LogicalPlan::SelectLiteral {
            values,
            filter: parse_predicates(&select.selection, params)?,
            deferred,
        });
    }
    let (table_name, table_alias) =
        if let Some(spec) = table_fn_from_factor(&select.from[0].relation, params) {
            // A set-returning function as the sole driving relation (e.g.
            // `FROM generate_series(1, 5)`): materialize it like a CTE and reference
            // it by alias.
            let alias = spec.alias.clone().unwrap_or_else(|| spec.name.clone());
            ctes.push((alias.clone(), Box::new(LogicalPlan::TableFunction(spec))));
            (alias, None)
        } else {
            match &select.from[0].relation {
                TableFactor::Table { name, alias, .. } => (
                    name.to_string(),
                    alias.as_ref().map(|a| a.name.value.clone()),
                ),
                TableFactor::Derived {
                    subquery, alias, ..
                } => {
                    let alias = alias
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("Derived table requires an alias"))?
                        .name
                        .value
                        .clone();
                    let sub_plan = plan_query(subquery, params)?;
                    ctes.push((alias.clone(), Box::new(sub_plan)));
                    (alias, None)
                }
                other => anyhow::bail!("Unsupported FROM relation: {:?}", other),
            }
        };

    let mut joins = Vec::new();
    for j in &select.from[0].joins {
        // A table function on the right of a join (incl. `CROSS JOIN LATERAL`) is
        // evaluated per driving row by the executor.
        if let Some(spec) = table_fn_from_factor(&j.relation, params) {
            let join_type = match &j.join_operator {
                JoinOperator::LeftOuter(_) | JoinOperator::Left(_) => JoinType::LeftOuter,
                _ => JoinType::Inner,
            };
            let alias = spec.alias.clone().unwrap_or_else(|| spec.name.clone());
            joins.push(crate::Join {
                table_name: alias.clone(),
                table_alias: Some(alias),
                condition: None,
                join_type,
                using_columns: Vec::new(),
                natural: false,
                table_fn: Some(spec),
            });
            continue;
        }
        let (join_table_name, join_table_alias) = match &j.relation {
            TableFactor::Table { name, alias, .. } => (
                name.to_string(),
                alias.as_ref().map(|a| a.name.value.clone()),
            ),
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let alias = alias
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Derived join requires an alias"))?
                    .name
                    .value
                    .clone();
                let sub_plan = plan_query(subquery, params)?;
                ctes.push((alias.clone(), Box::new(sub_plan)));
                (alias, None)
            }
            other => anyhow::bail!("Unsupported join relation: {:?}", other),
        };
        let (join_type, condition, using_columns, natural) = match &j.join_operator {
            // 0.62 distinguishes the bare keyword forms (`JOIN`, `LEFT JOIN`,
            // `RIGHT JOIN`) from the explicit `... OUTER JOIN` spellings; both map
            // to the same join type.
            JoinOperator::Join(c) | JoinOperator::Inner(c) => {
                join_constraint(JoinType::Inner, c, params)?
            }
            JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => {
                join_constraint(JoinType::LeftOuter, c, params)?
            }
            JoinOperator::Right(c) | JoinOperator::RightOuter(c) => {
                join_constraint(JoinType::RightOuter, c, params)?
            }
            JoinOperator::FullOuter(c) => join_constraint(JoinType::FullOuter, c, params)?,
            JoinOperator::CrossJoin(_) => (JoinType::Cross, None, Vec::new(), false),
            other => anyhow::bail!("Unsupported join operator: {:?}", other),
        };
        joins.push(crate::Join {
            table_name: join_table_name,
            table_alias: join_table_alias,
            condition,
            join_type,
            using_columns,
            natural,
            table_fn: None,
        });
    }

    // Comma-separated `FROM a, b, ...` items become cross joins. This is how
    // PostgreSQL clients (and introspection) write a lateral table function, e.g.
    // `FROM pg_index i, unnest(i.indkey) WITH ORDINALITY`.
    for twj in &select.from[1..] {
        if let Some(spec) = table_fn_from_factor(&twj.relation, params) {
            let alias = spec.alias.clone().unwrap_or_else(|| spec.name.clone());
            joins.push(crate::Join {
                table_name: alias.clone(),
                table_alias: Some(alias),
                condition: None,
                join_type: JoinType::Cross,
                using_columns: Vec::new(),
                natural: false,
                table_fn: Some(spec),
            });
            continue;
        }
        match &twj.relation {
            TableFactor::Table { name, alias, .. } => joins.push(crate::Join {
                table_name: name.to_string(),
                table_alias: alias.as_ref().map(|a| a.name.value.clone()),
                condition: None,
                join_type: JoinType::Cross,
                using_columns: Vec::new(),
                natural: false,
                table_fn: None,
            }),
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let alias = alias
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Derived table requires an alias"))?
                    .name
                    .value
                    .clone();
                let sub_plan = plan_query(subquery, params)?;
                ctes.push((alias.clone(), Box::new(sub_plan)));
                joins.push(crate::Join {
                    table_name: alias.clone(),
                    table_alias: Some(alias),
                    condition: None,
                    join_type: JoinType::Cross,
                    using_columns: Vec::new(),
                    natural: false,
                    table_fn: None,
                });
            }
            other => anyhow::bail!("Unsupported FROM relation: {:?}", other),
        }
    }

    // Projection: `*` -> empty (all); otherwise plain column identifiers.
    let mut projection = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                projection.clear();
                break;
            }
            SelectItem::UnnamedExpr(expr) => {
                projection.push(plan_select_expr(expr, None, params)?);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                projection.push(plan_select_expr(expr, Some(alias.value.clone()), params)?);
            }
            SelectItem::QualifiedWildcard(_, _) => {
                // `t.*` ideally projects only table `t`'s columns; NodusDB models
                // "all columns" as an empty projection, so treat it like `*`
                // rather than erroring. Correct for the common single-`t.*` case;
                // a mixed `t.*, expr` projection widens to all columns.
                projection.clear();
                break;
            }
            SelectItem::ExprWithAliases { .. } => {
                anyhow::bail!("Unsupported multi-alias select item")
            }
        }
    }

    let mut group_by = Vec::new();
    let mut grouping_sets: Option<Vec<Vec<String>>> = None;
    match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => {
            use sqlparser::ast::Expr;
            // Flattens a grouping element's expressions into column names.
            let cols_of =
                |es: &[Expr]| -> Vec<String> { es.iter().filter_map(extract_col_name).collect() };
            for expr in exprs {
                match expr {
                    Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                        if let Some(c) = extract_col_name(expr) {
                            group_by.push(c);
                        }
                    }
                    // Ordinal reference (`GROUP BY 1`).
                    Expr::Value(v) => {
                        if let sqlparser::ast::Value::Number(n, _) = &v.value {
                            if let Some(c) = n
                                .parse::<usize>()
                                .ok()
                                .and_then(|n| ordinal_target(&projection, n))
                            {
                                group_by.push(c);
                            }
                        }
                    }
                    // `ROLLUP(e1, e2, …)` → prefixes: {e1..en}, …, {e1}, {}.
                    Expr::Rollup(elements) => {
                        let elems: Vec<Vec<String>> = elements.iter().map(|e| cols_of(e)).collect();
                        let mut sets = Vec::new();
                        for i in (0..=elems.len()).rev() {
                            sets.push(elems[..i].concat());
                        }
                        grouping_sets = Some(sets);
                    }
                    // `CUBE(e1, …, en)` → every subset of the elements.
                    Expr::Cube(elements) => {
                        let elems: Vec<Vec<String>> = elements.iter().map(|e| cols_of(e)).collect();
                        let mut sets = Vec::new();
                        for mask in (0..(1u32 << elems.len())).rev() {
                            let mut set = Vec::new();
                            for (bit, e) in elems.iter().enumerate() {
                                if mask & (1 << bit) != 0 {
                                    set.extend(e.iter().cloned());
                                }
                            }
                            sets.push(set);
                        }
                        grouping_sets = Some(sets);
                    }
                    // `GROUPING SETS((a,b), (a), ())` → each inner list is one set.
                    Expr::GroupingSets(list) => {
                        grouping_sets = Some(list.iter().map(|e| cols_of(e)).collect());
                    }
                    _ => {}
                }
            }
            // `group_by` carries the union of every column mentioned (in first
            // appearance) so the output/NULL-rollup logic can resolve them.
            if let Some(sets) = &grouping_sets {
                for set in sets {
                    for c in set {
                        if !group_by.contains(c) {
                            group_by.push(c.clone());
                        }
                    }
                }
            }
        }
        _ => {}
    }

    // ORDER BY first column, if present.
    let order_by = match &query.order_by {
        Some(OrderBy {
            kind: OrderByKind::Expressions(exprs),
            ..
        }) => {
            let directions = exprs
                .iter()
                .map(|o| sort_ascending(&o.options))
                .collect::<Result<Vec<_>>>()?;
            exprs
                .iter()
                .zip(directions)
                .filter_map(|(o, asc)| match &o.expr {
                    // Bare or qualified (`t.col`) column reference.
                    Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                        extract_col_name(&o.expr).map(|col| (col, asc, o.options.nulls_first))
                    }
                    // Ordinal reference (`ORDER BY 1`).
                    Expr::Value(v) => match &v.value {
                        sqlparser::ast::Value::Number(n, _) => n
                            .parse::<usize>()
                            .ok()
                            .and_then(|n| ordinal_target(&projection, n))
                            .map(|col| (col, asc, o.options.nulls_first)),
                        _ => None,
                    },
                    _ => None,
                })
                .collect()
        }
        _ => Vec::new(),
    };

    // LIMIT <n> / OFFSET <n>, both carried by the `LimitClause`.
    let (limit_expr, offset_expr) = match &query.limit_clause {
        Some(LimitClause::LimitOffset { limit, offset, .. }) => {
            (limit.as_ref(), offset.as_ref().map(|o| &o.value))
        }
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => (Some(limit), Some(offset)),
        None => (None, None),
    };
    let limit = limit_expr
        .and_then(|e| expr_to_value(e, params).and_then(|v| render(&v).parse::<usize>().ok()));
    let offset = offset_expr
        .and_then(|e| expr_to_value(e, params).and_then(|v| render(&v).parse::<usize>().ok()));

    let distinct = select.distinct.is_some();

    let having = select
        .having
        .as_ref()
        .map(|expr| parse_having(expr, params))
        .transpose()?;

    Ok(LogicalPlan::Select {
        ctes,
        table_name,
        table_alias,
        joins,
        projection,
        group_by,
        filter: parse_predicates(&select.selection, params)?,
        having,
        grouping_sets,
        order_by,
        limit,
        offset,
        distinct,
    })
}

/// Translates a parsed JOIN constraint into NodusDB's join representation:
/// `(join_type, ON-condition, USING-columns, natural)`. `ON` becomes a filter
/// condition; `USING (cols)` and `NATURAL` carry their column intent for the
/// executor to resolve against the actual row schemas (so they compose with
/// chained joins, where the left input spans several tables).
fn join_constraint(
    join_type: JoinType,
    constraint: &sqlparser::ast::JoinConstraint,
    params: &[Value],
) -> Result<(JoinType, Option<FilterExpr>, Vec<String>, bool)> {
    use sqlparser::ast::JoinConstraint;
    Ok(match constraint {
        JoinConstraint::On(expr) => (
            join_type,
            Some(parse_filter_expr(expr, params)?),
            Vec::new(),
            false,
        ),
        JoinConstraint::Using(cols) => (
            join_type,
            None,
            cols.iter().map(|c| c.to_string()).collect(),
            false,
        ),
        JoinConstraint::Natural => (join_type, None, Vec::new(), true),
        JoinConstraint::None => (join_type, None, Vec::new(), false),
    })
}

/// Recognizes a set-returning function used as a `FROM`/join relation —
/// `unnest(...)`, `generate_series(...)` (any `name(args)` call), plus the
/// BigQuery `UNNEST([...])` form — capturing its arguments, `WITH ORDINALITY`
/// flag, and alias/column names. Returns `None` for an ordinary table.
fn table_fn_from_factor(
    factor: &sqlparser::ast::TableFactor,
    params: &[Value],
) -> Option<TableFnSpec> {
    use sqlparser::ast::TableFactor;
    match factor {
        TableFactor::Table {
            name,
            args: Some(table_args),
            with_ordinality,
            alias,
            ..
        } => {
            let args = table_args
                .args
                .iter()
                .filter_map(|a| function_arg_to_operand(a, params))
                .collect();
            Some(build_table_fn_spec(
                name.to_string().to_lowercase(),
                args,
                *with_ordinality,
                alias.as_ref(),
            ))
        }
        TableFactor::UNNEST {
            array_exprs,
            with_ordinality,
            alias,
            ..
        } => {
            let args = array_exprs
                .iter()
                .filter_map(|e| expr_to_operand(e, params))
                .collect();
            Some(build_table_fn_spec(
                "unnest".to_string(),
                args,
                *with_ordinality,
                alias.as_ref(),
            ))
        }
        _ => None,
    }
}

fn build_table_fn_spec(
    name: String,
    args: Vec<Operand>,
    with_ordinality: bool,
    alias: Option<&sqlparser::ast::TableAlias>,
) -> TableFnSpec {
    TableFnSpec {
        name,
        args,
        with_ordinality,
        alias: alias.map(|a| a.name.value.clone()),
        column_aliases: alias
            .map(|a| a.columns.iter().map(|c| c.name.value.clone()).collect())
            .unwrap_or_default(),
    }
}

fn function_arg_to_operand(arg: &sqlparser::ast::FunctionArg, params: &[Value]) -> Option<Operand> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr};
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => expr_to_operand(e, params),
        _ => None,
    }
}

/// A `FROM`-function argument is either a column reference (lateral — resolved
/// per driving row) or a constant/parameter value.
fn expr_to_operand(expr: &sqlparser::ast::Expr, params: &[Value]) -> Option<Operand> {
    if let Some(col) = extract_col_name(expr) {
        Some(Operand::Ident(col))
    } else {
        expr_to_value(expr, params).map(Operand::Literal)
    }
}

#[cfg(test)]
mod recursion_tests {
    /// A pathologically nested query must be rejected with a clean error rather
    /// than overflowing the stack (which is uncatchable and aborts the process).
    /// The rejection may come from sqlparser's own parse-time recursion limit or
    /// from `plan_query`'s depth guard; either is a safe, non-crashing outcome.
    #[test]
    fn deeply_nested_query_is_rejected_without_overflow() {
        let mut sql = String::from("SELECT 1");
        for _ in 0..400 {
            sql = format!("SELECT * FROM ({sql}) t");
        }
        let result = (|| {
            let mut stmts = nodus_sql::parse_sql(&sql)?;
            super::plan_statement(&stmts.remove(0), &[])
                .map_err(|e| sqlparser::parser::ParserError::ParserError(e.to_string()))
        })();
        assert!(
            result.is_err(),
            "deeply nested query should be rejected, not planned"
        );
    }
}

/// A call to a built-in non-aggregate function, lowered to a scalar
/// expression; `None` for aggregates and functions the library lacks.
fn known_function_call(
    expr: &sqlparser::ast::Expr,
    upper_name: &str,
    params: &[Value],
) -> Option<ScalarExpr> {
    let name = upper_name.strip_prefix("PG_CATALOG.").unwrap_or(upper_name);
    if aggregate_op(name).is_some() || !crate::functions::is_known(name) {
        return None;
    }
    lower_scalar(expr, params)
}

/// The first column a scalar expression references, if any.
fn first_column_reference(expr: &ScalarExpr) -> Option<&str> {
    match expr {
        ScalarExpr::Column(name) => Some(name.as_str()),
        _ => expr.children().into_iter().find_map(first_column_reference),
    }
}

/// PostgreSQL's default name for an unaliased select item (`FigureColname`):
/// a column's name, a function's name, a cast's type when it casts a constant,
/// a keyword for CASE/ARRAY/ROW/EXISTS, and `?column?` otherwise.
pub(crate) fn default_output_name(expr: &sqlparser::ast::Expr) -> String {
    figure_colname(expr).unwrap_or_else(|| "?column?".to_string())
}

fn figure_colname(expr: &sqlparser::ast::Expr) -> Option<String> {
    use sqlparser::ast::{AccessExpr, Expr, ObjectNamePart, TrimWhereField};
    Some(match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(parts) => parts.last()?.value.clone(),
        Expr::CompoundFieldAccess { root, access_chain } => {
            match access_chain.iter().rev().find_map(|access| match access {
                AccessExpr::Dot(Expr::Identifier(field)) => Some(field.value.clone()),
                _ => None,
            }) {
                Some(field) => field,
                None => return figure_colname(root),
            }
        }
        Expr::Nested(inner) | Expr::Collate { expr: inner, .. } => return figure_colname(inner),
        Expr::Function(function) => {
            let ObjectNamePart::Identifier(ident) = function.name.0.last()? else {
                return None;
            };
            if ident.quote_style.is_some() {
                ident.value.clone()
            } else {
                ident.value.to_ascii_lowercase()
            }
        }
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => figure_colname(inner)
            .filter(|name| name != "case")
            .unwrap_or_else(|| cast_type_name(&data_type.to_string())),
        Expr::TypedString(typed) => cast_type_name(&typed.data_type.to_string()),
        Expr::Interval(_) => "interval".to_string(),
        Expr::Case { .. } => "case".to_string(),
        Expr::Array(_) => "array".to_string(),
        Expr::Tuple(_) => "row".to_string(),
        Expr::Exists { .. } => "exists".to_string(),
        Expr::Subquery(query) => return query_output_name(query),
        Expr::Extract { .. } => "extract".to_string(),
        Expr::Position { .. } => "position".to_string(),
        Expr::Substring { shorthand, .. } => {
            if *shorthand { "substr" } else { "substring" }.to_string()
        }
        Expr::Overlay { .. } => "overlay".to_string(),
        Expr::Ceil { .. } => "ceil".to_string(),
        Expr::Floor { .. } => "floor".to_string(),
        Expr::Trim { trim_where, .. } => match trim_where {
            Some(TrimWhereField::Leading) => "ltrim",
            Some(TrimWhereField::Trailing) => "rtrim",
            _ => "btrim",
        }
        .to_string(),
        _ => return None,
    })
}

/// The name of a subquery's first output column, as a scalar subquery item
/// takes it.
fn query_output_name(query: &sqlparser::ast::Query) -> Option<String> {
    use sqlparser::ast::{SelectItem, SetExpr};
    let mut body = &*query.body;
    loop {
        match body {
            SetExpr::Select(select) => {
                return match select.projection.first()? {
                    SelectItem::UnnamedExpr(expr) => Some(default_output_name(expr)),
                    SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                    _ => None,
                };
            }
            SetExpr::Query(inner) => body = &inner.body,
            SetExpr::SetOperation { left, .. } => body = left,
            _ => return None,
        }
    }
}

/// PostgreSQL's internal name for a type as written in a cast (`int4` for
/// `integer`, `float8` for `double precision`).
fn cast_type_name(data_type: &str) -> String {
    let upper = data_type.trim().to_ascii_uppercase();
    let base = upper
        .trim_end_matches("[]")
        .split('(')
        .next()
        .unwrap_or_default()
        .trim();
    match base {
        "INT" | "INTEGER" | "INT4" => "int4",
        "BIGINT" | "INT8" => "int8",
        "SMALLINT" | "INT2" => "int2",
        "BOOL" | "BOOLEAN" => "bool",
        "REAL" | "FLOAT4" => "float4",
        "FLOAT" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => "float8",
        "DECIMAL" | "DEC" | "NUMERIC" => "numeric",
        "VARCHAR" | "CHARACTER VARYING" => "varchar",
        "CHAR" | "CHARACTER" | "BPCHAR" => "bpchar",
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => "timestamp",
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => "timestamptz",
        "TIME" | "TIME WITHOUT TIME ZONE" => "time",
        "TIMETZ" | "TIME WITH TIME ZONE" => "timetz",
        other => return other.to_ascii_lowercase().trim_matches('"').to_string(),
    }
    .to_string()
}

/// Plans one select-list expression; `alias` is its `AS` name. Plain columns,
/// literals, and plain aggregates keep their dedicated items; everything else
/// is a scalar expression named as PostgreSQL names it.
fn plan_select_expr(
    expr: &sqlparser::ast::Expr,
    alias: Option<String>,
    params: &[Value],
) -> Result<ProjectionItem> {
    use sqlparser::ast::Expr;
    let name = || alias.clone().unwrap_or_else(|| default_output_name(expr));
    if let Expr::Function(func) = expr
        && let Some(over) = &func.over
    {
        let mut partition_by = Vec::new();
        let mut order_by = Vec::new();
        let mut frame = None;
        if let sqlparser::ast::WindowType::WindowSpec(spec) = over {
            for expr in &spec.partition_by {
                if let Some(col) = extract_col_name(expr) {
                    partition_by.push(col);
                }
            }
            for expr in &spec.order_by {
                let asc = sort_ascending(&expr.options)?;
                if let Some(col) = extract_col_name(&expr.expr) {
                    order_by.push((col, asc));
                }
            }
            frame = window_frame(spec);
        }
        return Ok(ProjectionItem::WindowFunction {
            func_name: func.name.to_string().to_uppercase(),
            args: window_args(func),
            partition_by,
            order_by,
            alias: Some(name()),
            frame,
        });
    }
    if let Expr::Subquery(query) = expr {
        return Ok(ProjectionItem::Subquery {
            plan: Box::new(plan_query(query, params)?),
            alias: Some(name()),
        });
    }
    match lower_scalar(expr, params) {
        Some(ScalarExpr::Column(column)) => Ok(match alias {
            Some(alias) => ProjectionItem::AliasedColumn(column, alias),
            None => ProjectionItem::Column(column),
        }),
        Some(ScalarExpr::Literal(value)) if matches!(expr, Expr::Value(_)) => Ok(match alias {
            Some(alias) => ProjectionItem::AliasedLiteral(value, alias),
            None => ProjectionItem::Literal(value),
        }),
        Some(ScalarExpr::Aggregate {
            op,
            arg,
            arg_expr: None,
            distinct: false,
        }) if alias.is_none() => Ok(ProjectionItem::Aggregate(op, arg)),
        Some(scalar) => Ok(ProjectionItem::Expr {
            expr: scalar,
            alias: Some(name()),
        }),
        None => {
            // Catalog introspection calls functions NodusDB does not provide
            // over virtual tables; the executor resolves (or rejects) them.
            if let Expr::Function(func) = expr
                && unknown_function_error(expr).is_some()
            {
                let fname = func.name.to_string().to_uppercase();
                let mut args = Vec::new();
                if let sqlparser::ast::FunctionArguments::List(list) = &func.args {
                    for arg in &list.args {
                        if let sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) = arg
                        {
                            if let Some(col) = extract_col_name(e) {
                                args.push(col);
                            } else if let Some(val) = expr_to_value(e, params) {
                                args.push(literal_arg(&val));
                            }
                        }
                    }
                }
                return Ok(ProjectionItem::ScalarFunction {
                    func_name: fname,
                    args,
                    alias: Some(name()),
                });
            }
            Err(anyhow::anyhow!(
                unknown_function_error(expr)
                    .unwrap_or_else(|| format!("Unsupported expression in SELECT: {expr}"))
            ))
        }
    }
}
