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

pub(crate) fn plan_query(query: &sqlparser::ast::Query, params: &[Value]) -> Result<LogicalPlan> {
    use sqlparser::ast::*;

    let _depth_guard = PlanDepthGuard::enter()?;

    let mut ctes = Vec::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            let cte_name = cte.alias.name.value.clone();
            let cte_plan = plan_query(&cte.query, params)?;
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
        for item in &select.projection {
            let (expr, alias) = match item {
                SelectItem::UnnamedExpr(expr) => (expr, "?column?".to_string()),
                SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.to_string()),
                _ => anyhow::bail!("Unsupported scalar select item"),
            };
            if let Some(val) = expr_to_value(expr, params) {
                values.push((alias, val));
            } else if let Some(val) = fold_scalar(expr, params) {
                // Computed constant expressions: arithmetic, comparisons, CAST,
                // string concat, and scalar function calls. (Legacy handling
                // below still covers niladic specials like version().)
                values.push((alias, val));
            } else if let Expr::Function(func) = expr {
                let func_name = func.name.to_string();
                let rendered = if func_name.eq_ignore_ascii_case("version") {
                    "PostgreSQL 18.0 (NodusDB)".to_string()
                } else if func_name.eq_ignore_ascii_case("current_database") {
                    "default".to_string()
                } else if func_name.eq_ignore_ascii_case("current_schema") {
                    "public".to_string()
                } else if func_name.eq_ignore_ascii_case("current_user") {
                    "nodus".to_string()
                } else if func_name.eq_ignore_ascii_case("current_schemas") {
                    "{public}".to_string()
                } else if func_name.eq_ignore_ascii_case("round") {
                    "0".to_string()
                } else {
                    func_name
                };
                values.push((alias, crate::Value::Text(rendered)));
            } else if let Expr::Identifier(id) = expr {
                let rendered = if id.value.eq_ignore_ascii_case("current_user") {
                    "nodus".to_string()
                } else {
                    id.value.to_string()
                };
                values.push((alias, crate::Value::Text(rendered)));
            } else {
                values.push((alias, crate::Value::Int(0)));
            }
        }
        return Ok(LogicalPlan::SelectLiteral { values });
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
                join_constraint(JoinType::Inner, c, params)
            }
            JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => {
                join_constraint(JoinType::LeftOuter, c, params)
            }
            JoinOperator::Right(c) | JoinOperator::RightOuter(c) => {
                join_constraint(JoinType::RightOuter, c, params)
            }
            JoinOperator::FullOuter(c) => join_constraint(JoinType::FullOuter, c, params),
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
                if let Expr::Function(func) = expr {
                    let fname = func.name.to_string().to_uppercase();
                    if let Some(over) = &func.over {
                        let mut partition_by = Vec::new();
                        let mut order_by = Vec::new();
                        if let sqlparser::ast::WindowType::WindowSpec(spec) = over {
                            for expr in &spec.partition_by {
                                if let Some(col) = extract_col_name(expr) {
                                    partition_by.push(col);
                                }
                            }
                            for expr in &spec.order_by {
                                if let Some(col) = extract_col_name(&expr.expr) {
                                    order_by.push((col, expr.options.asc.unwrap_or(true)));
                                }
                            }
                        }
                        projection.push(ProjectionItem::WindowFunction {
                            func_name: fname,
                            args: window_args(func),
                            partition_by,
                            order_by,
                            alias: None,
                        });
                    } else if fname.starts_with("PG_CATALOG.")
                        || fname.starts_with("PG_")
                        || fname.eq_ignore_ascii_case("FORMAT_TYPE")
                    {
                        // Dummy handling for system functions during introspection, just treat it as a string literal
                        projection.push(ProjectionItem::Column(fname));
                    } else {
                        match fname.as_str() {
                            "COUNT" | "SUM" | "MIN" | "MAX" => {
                                let op = match fname.as_str() {
                                    "COUNT" => AggregateOp::Count,
                                    "SUM" => AggregateOp::Sum,
                                    "MIN" => AggregateOp::Min,
                                    "MAX" => AggregateOp::Max,
                                    _ => unreachable!(),
                                };
                                let first_arg = match &func.args {
                                    sqlparser::ast::FunctionArguments::List(list) => {
                                        list.args.first()
                                    }
                                    _ => None,
                                };
                                let inner = if let Some(arg) = first_arg {
                                    match arg {
                                        sqlparser::ast::FunctionArg::Unnamed(
                                            sqlparser::ast::FunctionArgExpr::Expr(
                                                Expr::Identifier(id),
                                            ),
                                        ) => id.value.clone(),
                                        sqlparser::ast::FunctionArg::Unnamed(
                                            sqlparser::ast::FunctionArgExpr::Wildcard,
                                        ) => "*".to_string(),
                                        _ => anyhow::bail!("Unsupported aggregate argument"),
                                    }
                                } else {
                                    anyhow::bail!("Aggregate function requires an argument");
                                };
                                projection.push(ProjectionItem::Aggregate(op, inner));
                            }
                            _ => {
                                let mut args = Vec::new();
                                let func_arg_list: &[sqlparser::ast::FunctionArg] = match &func.args
                                {
                                    sqlparser::ast::FunctionArguments::List(list) => {
                                        list.args.as_slice()
                                    }
                                    _ => &[],
                                };
                                for arg in func_arg_list {
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
                                projection.push(ProjectionItem::ScalarFunction {
                                    func_name: fname.clone(),
                                    args,
                                    alias: None, // Will fix later for ExprWithAlias
                                });
                            }
                        }
                    }
                } else if let Expr::BinaryOp { left, op, right } = expr
                    && matches!(
                        op,
                        BinaryOperator::Arrow
                            | BinaryOperator::LongArrow
                            | BinaryOperator::HashArrow
                            | BinaryOperator::HashLongArrow
                    )
                {
                    let left_col = extract_col_name(left)
                        .ok_or_else(|| anyhow::anyhow!("Invalid JSON left"))?;
                    let right_val = match &**right {
                        Expr::Value(v) => match &v.value {
                            sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
                            sqlparser::ast::Value::Number(n, _) => n.clone(),
                            _ => anyhow::bail!("Unsupported JSON path"),
                        },
                        _ => anyhow::bail!("Unsupported JSON path"),
                    };
                    let op_str = match op {
                        BinaryOperator::LongArrow => "->>",
                        BinaryOperator::Arrow => "->",
                        BinaryOperator::HashArrow => "#>",
                        BinaryOperator::HashLongArrow => "#>>",
                        _ => anyhow::bail!("Unsupported JSON operator"),
                    };
                    projection.push(ProjectionItem::JsonAccess {
                        left: left_col,
                        operator: op_str.to_string(),
                        right: right_val,
                        alias: None,
                    });
                } else if let Expr::Case { .. } = expr {
                    if let Some(case_projection) = parse_case(expr, None, params) {
                        projection.push(case_projection);
                    } else {
                        projection.push(ProjectionItem::Literal(crate::Value::Null));
                    }
                } else if let Expr::Substring {
                    expr: inner,
                    substring_from,
                    substring_for,
                    ..
                } = expr
                {
                    // sqlparser 0.62 parses `SUBSTR(x, a, b)` as a dedicated
                    // `Substring` node; map it back to the SUBSTR scalar function.
                    projection.push(ProjectionItem::ScalarFunction {
                        func_name: "SUBSTR".to_string(),
                        args: substring_args(inner, substring_from, substring_for, params),
                        alias: None,
                    });
                } else if let Some(col) = extract_col_name(expr) {
                    projection.push(ProjectionItem::Column(col));
                } else if let Some(val) = expr_to_value(expr, params) {
                    projection.push(ProjectionItem::Literal(val));
                } else if matches!(expr, Expr::Array(_)) {
                    // An `ARRAY[...]` of literals folds to a constant; one with
                    // column refs (e.g. `ARRAY[d.objsubid]` in pg_depend
                    // introspection) can't be folded here, so surface NULL rather
                    // than failing the statement.
                    let val = expr_to_value(expr, params).unwrap_or(crate::Value::Null);
                    projection.push(ProjectionItem::Literal(val));
                } else {
                    anyhow::bail!("Unsupported projection item: {:?}", item);
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Expr::Function(func) = expr {
                    let fname = func.name.to_string().to_uppercase();
                    if let Some(over) = &func.over {
                        let mut partition_by = Vec::new();
                        let mut order_by = Vec::new();
                        if let sqlparser::ast::WindowType::WindowSpec(spec) = over {
                            for expr in &spec.partition_by {
                                if let Some(col) = extract_col_name(expr) {
                                    partition_by.push(col);
                                }
                            }
                            for expr in &spec.order_by {
                                if let Some(col) = extract_col_name(&expr.expr) {
                                    order_by.push((col, expr.options.asc.unwrap_or(true)));
                                }
                            }
                        }
                        projection.push(ProjectionItem::WindowFunction {
                            func_name: fname,
                            args: window_args(func),
                            partition_by,
                            order_by,
                            alias: Some(alias.value.clone()),
                        });
                    } else if fname.starts_with("PG_CATALOG.")
                        || fname.starts_with("PG_")
                        || fname.eq_ignore_ascii_case("FORMAT_TYPE")
                    {
                        projection.push(ProjectionItem::AliasedColumn(fname, alias.value.clone()));
                    } else {
                        match fname.as_str() {
                            "COUNT" | "SUM" | "MIN" | "MAX" => {
                                let op = match fname.as_str() {
                                    "COUNT" => AggregateOp::Count,
                                    "SUM" => AggregateOp::Sum,
                                    "MIN" => AggregateOp::Min,
                                    "MAX" => AggregateOp::Max,
                                    _ => unreachable!(),
                                };
                                let first_arg = match &func.args {
                                    sqlparser::ast::FunctionArguments::List(list) => {
                                        list.args.first()
                                    }
                                    _ => None,
                                };
                                let inner = if let Some(arg) = first_arg {
                                    match arg {
                                        sqlparser::ast::FunctionArg::Unnamed(
                                            sqlparser::ast::FunctionArgExpr::Expr(
                                                Expr::Identifier(id),
                                            ),
                                        ) => id.value.clone(),
                                        sqlparser::ast::FunctionArg::Unnamed(
                                            sqlparser::ast::FunctionArgExpr::Wildcard,
                                        ) => "*".to_string(),
                                        _ => anyhow::bail!("Unsupported aggregate argument"),
                                    }
                                } else {
                                    anyhow::bail!("Aggregate function requires an argument");
                                };
                                projection.push(ProjectionItem::Aggregate(op, inner));
                            }
                            _ => {
                                let mut args = Vec::new();
                                let func_arg_list: &[sqlparser::ast::FunctionArg] = match &func.args
                                {
                                    sqlparser::ast::FunctionArguments::List(list) => {
                                        list.args.as_slice()
                                    }
                                    _ => &[],
                                };
                                for arg in func_arg_list {
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
                                projection.push(ProjectionItem::ScalarFunction {
                                    func_name: fname.clone(),
                                    args,
                                    alias: Some(alias.value.clone()),
                                });
                            }
                        }
                    }
                } else if let Expr::BinaryOp { left, op, right } = expr
                    && matches!(
                        op,
                        BinaryOperator::Arrow
                            | BinaryOperator::LongArrow
                            | BinaryOperator::HashArrow
                            | BinaryOperator::HashLongArrow
                    )
                {
                    let left_col = extract_col_name(left)
                        .ok_or_else(|| anyhow::anyhow!("Invalid JSON left"))?;
                    let right_val = match &**right {
                        Expr::Value(v) => match &v.value {
                            sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
                            sqlparser::ast::Value::Number(n, _) => n.clone(),
                            _ => anyhow::bail!("Unsupported JSON path"),
                        },
                        _ => anyhow::bail!("Unsupported JSON path"),
                    };
                    let op_str = match op {
                        BinaryOperator::LongArrow => "->>",
                        BinaryOperator::Arrow => "->",
                        BinaryOperator::HashArrow => "#>",
                        BinaryOperator::HashLongArrow => "#>>",
                        _ => anyhow::bail!("Unsupported JSON operator"),
                    };
                    projection.push(ProjectionItem::JsonAccess {
                        left: left_col,
                        operator: op_str.to_string(),
                        right: right_val,
                        alias: Some(alias.value.clone()),
                    });
                } else if let Expr::Case { .. } = expr {
                    if let Some(case_projection) =
                        parse_case(expr, Some(alias.value.clone()), params)
                    {
                        projection.push(case_projection);
                    } else {
                        projection.push(ProjectionItem::AliasedLiteral(
                            crate::Value::Text("TABLE".to_string()),
                            alias.value.clone(),
                        ));
                    }
                } else if let Expr::Substring {
                    expr: inner,
                    substring_from,
                    substring_for,
                    ..
                } = expr
                {
                    projection.push(ProjectionItem::ScalarFunction {
                        func_name: "SUBSTR".to_string(),
                        args: substring_args(inner, substring_from, substring_for, params),
                        alias: Some(alias.value.clone()),
                    });
                } else if let Some(col) = extract_col_name(expr) {
                    projection.push(ProjectionItem::AliasedColumn(col, alias.value.clone()));
                } else if let Some(val) = expr_to_value(expr, params) {
                    projection.push(ProjectionItem::AliasedLiteral(val, alias.value.clone()));
                } else {
                    projection.push(ProjectionItem::AliasedLiteral(
                        crate::Value::Null,
                        alias.value.clone(),
                    ));
                }
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
    match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => {
            for expr in exprs {
                if let sqlparser::ast::Expr::Identifier(id) = expr {
                    group_by.push(id.value.clone());
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
        }) => exprs
            .iter()
            .filter_map(|o| match &o.expr {
                Expr::Identifier(id) => Some((id.value.clone(), o.options.asc.unwrap_or(true))),
                _ => None,
            })
            .collect(),
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
        .and_then(|expr| parse_filter_expr(expr, params));

    Ok(LogicalPlan::Select {
        ctes,
        table_name,
        table_alias,
        joins,
        projection,
        group_by,
        filter: parse_predicates(&select.selection, params),
        having,
        order_by,
        limit,
        offset,
        distinct,
    })
}

/// Builds the SUBSTR scalar-function argument strings from a parsed `Substring`
/// node (`SUBSTR(expr, from, for)`): each present operand resolves to a column
/// name or a rendered literal, matching how the planner captures other scalar
/// function arguments.
fn substring_args(
    inner: &sqlparser::ast::Expr,
    substring_from: &Option<Box<sqlparser::ast::Expr>>,
    substring_for: &Option<Box<sqlparser::ast::Expr>>,
    params: &[Value],
) -> Vec<String> {
    let mut args = Vec::new();
    let operands = [
        Some(inner),
        substring_from.as_deref(),
        substring_for.as_deref(),
    ];
    for e in operands.into_iter().flatten() {
        if let Some(col) = extract_col_name(e) {
            args.push(col);
        } else if let Some(val) = expr_to_value(e, params) {
            args.push(literal_arg(&val));
        }
    }
    args
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
) -> (JoinType, Option<FilterExpr>, Vec<String>, bool) {
    use sqlparser::ast::JoinConstraint;
    match constraint {
        JoinConstraint::On(expr) => (
            join_type,
            parse_filter_expr(expr, params),
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
    }
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
