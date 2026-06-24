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

pub(crate) fn plan_query(query: &sqlparser::ast::Query, params: &[Value]) -> Result<LogicalPlan> {
    use sqlparser::ast::*;

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
            SetOperator::Except => SetOpKind::Except,
        };
        let all = *set_quantifier == SetQuantifier::All;
        let wrap = |body: &Box<SetExpr>| Query {
            with: None,
            body: body.clone(),
            order_by: vec![],
            limit: None,
            limit_by: vec![],
            offset: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
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
    let (table_name, table_alias) = match &select.from[0].relation {
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
    };

    let mut joins = Vec::new();
    for j in &select.from[0].joins {
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
        let (join_type, condition) = match &j.join_operator {
            JoinOperator::Inner(JoinConstraint::On(expr)) => {
                (JoinType::Inner, parse_filter_expr(expr, params))
            }
            JoinOperator::LeftOuter(JoinConstraint::On(expr)) => {
                (JoinType::LeftOuter, parse_filter_expr(expr, params))
            }
            JoinOperator::RightOuter(JoinConstraint::On(expr)) => {
                (JoinType::RightOuter, parse_filter_expr(expr, params))
            }
            JoinOperator::FullOuter(JoinConstraint::On(expr)) => {
                (JoinType::FullOuter, parse_filter_expr(expr, params))
            }
            JoinOperator::CrossJoin => (JoinType::Cross, None),
            other => anyhow::bail!("Unsupported join operator: {:?}", other),
        };
        joins.push(crate::Join {
            table_name: join_table_name,
            table_alias: join_table_alias,
            condition,
            join_type,
        });
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
                                    order_by.push((col, expr.asc.unwrap_or(true)));
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
                                let inner = if let Some(arg) = func.args.first() {
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
                                for arg in &func.args {
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
                } else if let Expr::JsonAccess {
                    left,
                    operator,
                    right,
                } = expr
                {
                    let left_col = extract_col_name(left)
                        .ok_or_else(|| anyhow::anyhow!("Invalid JSON left"))?;
                    let right_val = match &**right {
                        Expr::Value(v) => match v {
                            sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
                            sqlparser::ast::Value::Number(n, _) => n.clone(),
                            _ => anyhow::bail!("Unsupported JSON path"),
                        },
                        _ => anyhow::bail!("Unsupported JSON path"),
                    };
                    let op_str = match operator {
                        sqlparser::ast::JsonOperator::LongArrow => "->>",
                        sqlparser::ast::JsonOperator::Arrow => "->",
                        sqlparser::ast::JsonOperator::HashArrow => "#>",
                        sqlparser::ast::JsonOperator::HashLongArrow => "#>>",
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
                } else if let Some(col) = extract_col_name(expr) {
                    projection.push(ProjectionItem::Column(col));
                } else if let Some(val) = expr_to_value(expr, params) {
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
                                    order_by.push((col, expr.asc.unwrap_or(true)));
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
                                let inner = if let Some(arg) = func.args.first() {
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
                                for arg in &func.args {
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
                } else if let Expr::JsonAccess {
                    left,
                    operator,
                    right,
                } = expr
                {
                    let left_col = extract_col_name(left)
                        .ok_or_else(|| anyhow::anyhow!("Invalid JSON left"))?;
                    let right_val = match &**right {
                        Expr::Value(v) => match v {
                            sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
                            sqlparser::ast::Value::Number(n, _) => n.clone(),
                            _ => anyhow::bail!("Unsupported JSON path"),
                        },
                        _ => anyhow::bail!("Unsupported JSON path"),
                    };
                    let op_str = match operator {
                        sqlparser::ast::JsonOperator::LongArrow => "->>",
                        sqlparser::ast::JsonOperator::Arrow => "->",
                        sqlparser::ast::JsonOperator::HashArrow => "#>",
                        sqlparser::ast::JsonOperator::HashLongArrow => "#>>",
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
                anyhow::bail!("Qualified wildcard not supported");
            }
        }
    }

    let mut group_by = Vec::new();
    match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs) => {
            for expr in exprs {
                if let sqlparser::ast::Expr::Identifier(id) = expr {
                    group_by.push(id.value.clone());
                }
            }
        }
        _ => {}
    }

    // ORDER BY first column, if present.
    let order_by = query
        .order_by
        .iter()
        .filter_map(|o| match &o.expr {
            Expr::Identifier(id) => Some((id.value.clone(), o.asc.unwrap_or(true))),
            _ => None,
        })
        .collect();

    // LIMIT <n>.
    let limit = query
        .limit
        .as_ref()
        .and_then(|e| expr_to_value(e, params).and_then(|v| render(&v).parse::<usize>().ok()));

    // OFFSET <n>.
    let offset = query.offset.as_ref().and_then(|o| {
        expr_to_value(&o.value, params).and_then(|v| render(&v).parse::<usize>().ok())
    });

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
