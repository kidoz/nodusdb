//! Query planning: SELECT/set-op planning and object-name resolution.
use super::*;
use crate::plan_types::{
    JsonTableColumn, JsonTableColumnKind, JsonTableSpec, XmlTableColumn, XmlTableColumnKind,
    XmlTableNamespace, XmlTableSpec,
};
use crate::*;
use anyhow::Result;
use nodus_catalog::TableConstraint;

pub(crate) fn table_name_of(relation: &sqlparser::ast::TableFactor) -> Result<String> {
    match relation {
        sqlparser::ast::TableFactor::Table { name, .. } => Ok(factor_only(name).0),
        other => anyhow::bail!("Unsupported table relation: {other}"),
    }
}

/// The table a data-modifying statement writes, and the alias it goes by.
pub(crate) fn target_table(
    relation: &sqlparser::ast::TableFactor,
) -> Result<(String, Option<String>, bool)> {
    match relation {
        sqlparser::ast::TableFactor::Table { name, alias, .. } => {
            let (name, only) = factor_only(name);
            Ok((name, table_alias_name(alias.as_ref())?, only))
        }
        other => anyhow::bail!("Unsupported table relation: {other}"),
    }
}

/// The name a table factor reads, and whether `ONLY` preceded it. The token
/// rewriter leaves `ONLY` behind as a marked name for the spellings the
/// parser cannot take (`FROM ONLY t AS x`), which is stripped here.
pub(crate) fn factor_only(name: &sqlparser::ast::ObjectName) -> (String, bool) {
    const MARK: &str = "\u{0}only:";
    let parts = &name.0;
    if let [sqlparser::ast::ObjectNamePart::Identifier(ident)] = parts.as_slice()
        && let Some(rest) = ident.value.strip_prefix(MARK)
    {
        return (rest.to_string(), true);
    }
    (name.to_string(), false)
}

pub fn parse_object_name(name: &str) -> Result<(&str, &str, &str)> {
    let parts: Vec<&str> = name.split('.').collect();
    match parts.len() {
        // An unqualified name is found along the session's search path.
        // The system catalogs come first, as pg_catalog is searched before
        // the path.
        1 if crate::MemExecutor::is_pg_catalog_virtual_table_name(parts[0].trim_matches('"')) => {
            Ok(("default", "pg_catalog", parts[0].trim_matches('"')))
        }
        1 => {
            let relation = parts[0].trim_matches('"');
            Ok((
                "default",
                crate::search_path::schema_for(relation),
                relation,
            ))
        }
        2 => Ok((
            "default",
            crate::search_path::schema_named(parts[0].trim_matches('"')),
            parts[1].trim_matches('"'),
        )),
        3 => Ok((
            parts[0].trim_matches('"'),
            crate::search_path::schema_named(parts[1].trim_matches('"')),
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

pub(crate) fn plan_query(query: &sqlparser::ast::Query, params: &[Value]) -> Result<LogicalPlan> {
    use sqlparser::ast::*;

    let _depth_guard = PlanDepthGuard::enter()?;

    let mut ctes = match &query.with {
        Some(with) => plan_ctes(with, params)?,
        None => Vec::new(),
    };

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
        let set_op = LogicalPlan::SetOp {
            op: kind,
            all,
            left: Box::new(left_plan),
            right: Box::new(right_plan),
        };
        return select_from_result(set_op, ctes, query, params);
    }

    // A data-modifying statement as a query (a CTE's body): its RETURNING
    // rows.
    if let SetExpr::Insert(statement)
    | SetExpr::Update(statement)
    | SetExpr::Delete(statement)
    | SetExpr::Merge(statement) = &*query.body
    {
        return super::plan_statement(statement, params);
    }

    // A parenthesized query, which may have its own ORDER BY and LIMIT
    // (`(SELECT ... LIMIT 2) UNION ALL (...)`).
    if let SetExpr::Query(inner) = &*query.body {
        let inner_plan = plan_query(inner, params)?;
        if ctes.is_empty()
            && query.order_by.is_none()
            && query.limit_clause.is_none()
            && query.fetch.is_none()
        {
            return Ok(inner_plan);
        }
        return select_from_result(inner_plan, ctes, query, params);
    }

    // A `VALUES` list as the query: `VALUES (1, 'a'), (2, 'b')`.
    if let SetExpr::Values(values) = &*query.body {
        let rows = values
            .rows
            .iter()
            .map(|row| {
                row.content
                    .iter()
                    .map(|e| {
                        lower_scalar(e, params).ok_or_else(|| {
                            expression_error(e, || format!("Unsupported expression in VALUES: {e}"))
                        })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        if rows.iter().any(|row| row.len() != rows[0].len()) {
            anyhow::bail!("VALUES lists must all be the same length");
        }
        let rows = typed_values_rows(rows);
        return select_from_result(LogicalPlan::Values { rows }, ctes, query, params);
    }

    let SetExpr::Select(select) = &*query.body else {
        anyhow::bail!("Unsupported query body");
    };
    // Its window calls may name the windows of its `WINDOW` clause.
    let _windows = named_windows_scope(&select.named_window);

    // A set-returning function as the whole select list (`SELECT
    // generate_series(1, 3)`) yields its rows, as from `FROM` would.
    if select.from.is_empty()
        && select.selection.is_none()
        && let [item] = select.projection.as_slice()
        && let Some(spec) = select_list_table_function(item, params)
    {
        return select_from_result(LogicalPlan::TableFunction(spec), ctes, query, params);
    }

    // Set-returning functions elsewhere in a FROM-less select list: the
    // query reads their rows.
    if select.from.is_empty() && select.projection.iter().any(select_item_has_srf) {
        let mut projection = Vec::new();
        for item in &select.projection {
            projection.push(match item {
                SelectItem::UnnamedExpr(expr) => plan_select_expr(expr, None, params)?,
                SelectItem::ExprWithAlias { expr, alias } => {
                    plan_select_expr(expr, Some(alias.value.clone()), params)?
                }
                other => anyhow::bail!("Unsupported select item: {other}"),
            });
        }
        let mut sort = plan_sort(query.order_by.as_ref(), &projection, false, params)?;
        let spec = lift_set_returning_functions(&mut projection, &mut sort)
            .expect("the select list calls a set-returning function");
        ctes.push((
            SRF_RELATION.to_string(),
            Box::new(LogicalPlan::TableFunction(spec)),
        ));
        let (limit, offset) = plan_limit(query, params, &mut sort)?;
        return Ok(LogicalPlan::Select {
            only: false,
            ctes,
            table_name: SRF_RELATION.to_string(),
            table_alias: None,
            joins: Vec::new(),
            projection,
            group_by: Vec::new(),
            filter: parse_predicates(&select.selection, params)?,
            having: None,
            grouping_sets: None,
            order_by: Vec::new(),
            limit,
            offset,
            distinct: false,
            sort,
            group_exprs: Vec::new(),
            distinct_on: Vec::new(),
            sample: None,
        });
    }

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
            let type_hint = match expr {
                Expr::Cast { data_type, .. } => Some(data_type.to_string()),
                Expr::TypedString(ts) => Some(ts.data_type.to_string()),
                Expr::Interval(_) => Some("INTERVAL".to_string()),
                _ => None,
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
                    let scalar = lower_scalar(expr, params).ok_or_else(|| {
                        expression_error(expr, || {
                            format!("Unsupported expression in SELECT: {expr}")
                        })
                    })?;
                    // A column reference is an enclosing query's (checked
                    // when the query runs, once those are bound).
                    DeferredItem::Scalar(scalar)
                }
            };
            values.push((alias, crate::Value::Null, type_hint));
            deferred.push(Some(item));
        }
        // Its WITH, ORDER BY, and LIMIT apply as for any other query.
        let literal = LogicalPlan::SelectLiteral {
            values,
            filter: parse_predicates(&select.selection, params)?,
            deferred,
        };
        return select_from_result(literal, ctes, query, params);
    }
    let (table_name, table_alias, mut joins, sample, only) =
        plan_from(&select.from, &mut ctes, params)?;

    // Projection: a lone `*` is empty (all columns); `*` among other items,
    // and `t.*`, stand for columns the executor expands once the relations'
    // columns are known.
    let mut projection = Vec::new();
    if !matches!(select.projection.as_slice(), [SelectItem::Wildcard(_)]) {
        for item in &select.projection {
            match item {
                SelectItem::Wildcard(_) => projection.push(ProjectionItem::Column("*".to_string())),
                SelectItem::UnnamedExpr(expr) => {
                    projection.push(plan_select_expr(expr, None, params)?);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    projection.push(plan_select_expr(expr, Some(alias.value.clone()), params)?);
                }
                SelectItem::QualifiedWildcard(
                    sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(name),
                    _,
                ) => {
                    let relation = name
                        .0
                        .iter()
                        .map(|part| part.to_string().trim_matches('"').to_string())
                        .collect::<Vec<_>>()
                        .join(".");
                    projection.push(ProjectionItem::Column(format!("{relation}.*")));
                }
                other => anyhow::bail!("Unsupported select item: {other}"),
            }
        }
    }

    let (distinct, distinct_on_exprs) = match &select.distinct {
        None | Some(sqlparser::ast::Distinct::All) => (false, &[][..]),
        Some(sqlparser::ast::Distinct::Distinct) => (true, &[][..]),
        Some(sqlparser::ast::Distinct::On(exprs)) => (false, exprs.as_slice()),
    };
    let (group_by, grouping_sets, group_exprs) =
        plan_group_by(&select.group_by, &projection, params)?;
    let mut sort = plan_sort(query.order_by.as_ref(), &projection, distinct, params)?;
    let distinct_on = distinct_on_exprs
        .iter()
        .map(|e| sort_target(e, &projection, params))
        .collect::<Result<Vec<_>>>()?;
    // As in PostgreSQL, the leading ORDER BY keys must be DISTINCT ON keys,
    // so "the first row of each key" is well defined.
    let same = |a: &SortTarget, b: &SortTarget| {
        canonical_target(a, &projection) == canonical_target(b, &projection)
    };
    if sort
        .iter()
        .take(distinct_on.len())
        .any(|key| !distinct_on.iter().any(|d| same(d, &key.target)))
    {
        anyhow::bail!("SELECT DISTINCT ON expressions must match initial ORDER BY expressions");
    }
    let (limit, offset) = plan_limit(query, params, &mut sort)?;

    let having = select
        .having
        .as_ref()
        .map(|expr| parse_having(expr, params))
        .transpose()?;

    // Set-returning functions in the select list run per input row, as a
    // lateral join whose columns stand in for the calls. Grouping and
    // window functions would have to run before them.
    let mut sort = sort;
    if let Some(spec) = lift_set_returning_functions(&mut projection, &mut sort) {
        let aggregated = !group_by.is_empty()
            || having.is_some()
            || projection.iter().any(|p| match p {
                ProjectionItem::Aggregate(..) | ProjectionItem::WindowFunction { .. } => true,
                ProjectionItem::Expr { expr, .. } => scalar_has_aggregate(expr),
                _ => false,
            });
        if aggregated {
            anyhow::bail!(
                "set-returning functions in a select list with aggregates or window functions are not supported"
            );
        }
        joins.push(crate::Join {
            only: false,
            table_name: SRF_RELATION.to_string(),
            table_alias: Some(SRF_RELATION.to_string()),
            condition: None,
            join_type: JoinType::Cross,
            table_fn: Some(spec),
            using_columns: Vec::new(),
            natural: false,
            lateral: None,
            sample: None,
        });
    }

    // With one comma-join, a `WHERE` condition over both sides is the
    // join's own (`Join Filter` / `Hash Cond`), as PostgreSQL plans it.
    let mut filter = parse_predicates(&select.selection, params)?;
    if joins.len() == 1
        && joins[0].table_fn.is_none()
        && joins[0].lateral.is_none()
        && joins[0].condition.is_none()
        && joins[0].using_columns.is_empty()
        && !joins[0].natural
        && matches!(joins[0].join_type, JoinType::Cross)
        && let Some(filter_expr) = &filter
    {
        let left = table_alias.as_deref().unwrap_or(&table_name).to_string();
        let right = joins[0]
            .table_alias
            .as_deref()
            .unwrap_or(&joins[0].table_name)
            .to_string();
        let mut hoisted = Vec::new();
        let mut remaining = Vec::new();
        for conjunct in crate::index_keys::conjuncts(filter_expr) {
            let mut refs = Vec::new();
            crate::filter_eval::filter_column_refs(conjunct, &mut refs);
            let on = |prefix: &str| {
                refs.iter().any(|reference| {
                    reference
                        .rsplit_once('.')
                        .is_some_and(|(qualifier, _)| qualifier == prefix)
                })
            };
            let qualified = refs.iter().all(|reference| reference.contains('.'));
            if qualified && on(&left) && on(&right) {
                hoisted.push(conjunct.clone());
            } else {
                remaining.push(conjunct.clone());
            }
        }
        if !hoisted.is_empty() {
            joins[0].condition = crate::joins::conjunction(&hoisted);
            filter = crate::joins::conjunction(&remaining);
        }
    }
    Ok(LogicalPlan::Select {
        only,
        ctes,
        table_name,
        table_alias,
        joins,
        projection,
        group_by,
        filter,
        having,
        grouping_sets,
        order_by: Vec::new(),
        limit,
        offset,
        distinct,
        sort,
        group_exprs,
        distinct_on,
        sample,
    })
}

/// Plans a `WITH` list: each CTE's query, recursive or renamed as declared.
pub(crate) fn plan_ctes(
    with: &sqlparser::ast::With,
    params: &[Value],
) -> Result<Vec<(String, Box<LogicalPlan>)>> {
    let mut ctes = Vec::new();
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
            // `WITH x(a, b) AS (...)` names the CTE's columns.
            other if !column_aliases.is_empty() => LogicalPlan::Renamed {
                input: Box::new(other),
                columns: column_aliases,
            },
            other => other,
        };
        ctes.push((cte_name, Box::new(cte_plan)));
    }
    Ok(ctes)
}

/// The `TABLESAMPLE` clause of a FROM factor, as the plan carries it.
/// PostgreSQL allows the clause on a plain table — and a materialized view,
/// which is a table here — and its grammar rejects every other shape; the
/// checks below reproduce those errors.
fn factor_sample(
    factor: &sqlparser::ast::TableFactor,
    params: &[Value],
) -> Result<Option<crate::SampleSpec>> {
    use sqlparser::ast::*;
    let (sample, plain_table) = match factor {
        TableFactor::Table { args, sample, .. } => (sample.as_ref(), args.is_none()),
        TableFactor::Derived { sample, .. } => (sample.as_ref(), false),
        _ => (None, false),
    };
    let Some(sample) = sample else {
        return Ok(None);
    };
    if !plain_table {
        anyhow::bail!(sample_syntax_error("tablesample"));
    }
    let sample = match sample {
        TableSampleKind::BeforeTableAlias(sample) | TableSampleKind::AfterTableAlias(sample) => {
            sample.as_ref()
        }
    };
    if sample.modifier != TableSampleModifier::TableSample {
        anyhow::bail!(sample_syntax_error("sample"));
    }
    let method = match &sample.name {
        Some(TableSampleMethod::Bernoulli) => "bernoulli",
        Some(TableSampleMethod::System) => "system",
        // `ROW` is a keyword, so PostgreSQL's grammar never reaches the
        // method check for it; `BLOCK` is a plain name and gets the check.
        Some(TableSampleMethod::Row) => anyhow::bail!(sample_syntax_error("(")),
        Some(TableSampleMethod::Block) => anyhow::bail!(method_error("block")),
        None => {
            // An unknown method is parsed as a call — `TABLESAMPLE vacuum
            // (10)` reads as the expression `vacuum(10)` — so its name is
            // recoverable from the quantity's expression.
            if let Some(TableSampleQuantity {
                value: Expr::Function(function),
                ..
            }) = &sample.quantity
            {
                anyhow::bail!(method_error(
                    &function.name.to_string().to_ascii_lowercase()
                ));
            }
            anyhow::bail!(sample_syntax_error("tablesample"));
        }
    };
    let quantity = match &sample.quantity {
        Some(quantity) if quantity.parenthesized => quantity,
        Some(quantity) => anyhow::bail!(sample_syntax_error(&quantity.value.to_string())),
        None => anyhow::bail!(sample_syntax_error("tablesample")),
    };
    if let Some(unit) = quantity.unit {
        let token = match unit {
            TableSampleUnit::Percent => "percent",
            TableSampleUnit::Rows => "rows",
        };
        anyhow::bail!(sample_syntax_error(token));
    }
    if sample.bucket.is_some() || sample.offset.is_some() {
        anyhow::bail!(sample_syntax_error("tablesample"));
    }
    if let Some(seed) = &sample.seed
        && seed.modifier != TableSampleSeedModifier::Repeatable
    {
        anyhow::bail!(sample_syntax_error("seed"));
    }
    let percent = lower_scalar(&quantity.value, params).ok_or_else(|| {
        anyhow::anyhow!("Unsupported expression in TABLESAMPLE: {}", quantity.value)
    })?;
    let seed = match &sample.seed {
        Some(seed) => Some(
            lower_scalar(&Expr::Value(seed.value.clone()), params).ok_or_else(|| {
                anyhow::anyhow!("Unsupported expression in TABLESAMPLE: {}", seed.value)
            })?,
        ),
        None => None,
    };
    Ok(Some(crate::SampleSpec {
        method: method.to_string(),
        percent,
        seed,
    }))
}

/// The syntax error (42601) PostgreSQL's grammar gives for a shape it does
/// not accept.
fn sample_syntax_error(token: &str) -> String {
    crate::error_fields::DbError::new(format!("syntax error at or near \"{token}\""))
        .code("42601")
        .into_text()
}

/// The error an unknown `TABLESAMPLE` method raises, as PostgreSQL words it.
fn method_error(name: &str) -> String {
    crate::error_fields::DbError::new(format!("tablesample method {name} does not exist"))
        .code("42704")
        .into_text()
}

/// Plans a FROM list: its first relation, and a join for each relation after
/// it. Derived tables and set-returning functions are added to `ctes`.
fn plan_from(
    from: &[sqlparser::ast::TableWithJoins],
    ctes: &mut Vec<(String, Box<LogicalPlan>)>,
    params: &[Value],
) -> Result<(
    String,
    Option<String>,
    Vec<crate::Join>,
    Option<crate::SampleSpec>,
    bool,
)> {
    use sqlparser::ast::*;
    let sample = factor_sample(&from[0].relation, params)?;
    let mut first_only = false;
    let (table_name, table_alias) =
        if let Some(spec) = table_fn_from_factor(&from[0].relation, params) {
            // A set-returning function as the sole driving relation (e.g.
            // `FROM generate_series(1, 5)`): materialize it like a CTE and reference
            // it by alias.
            let alias = spec.alias.clone().unwrap_or_else(|| spec.name.clone());
            ctes.push((alias.clone(), Box::new(LogicalPlan::TableFunction(spec))));
            (alias, None)
        } else {
            match &from[0].relation {
                TableFactor::Table { name, alias, .. } => {
                    let (name, only) = factor_only(name);
                    first_only = only;
                    (name, table_alias_name(alias.as_ref())?)
                }
                // The first relation has nothing to its left, so LATERAL means
                // nothing there.
                TableFactor::Derived {
                    subquery, alias, ..
                } => {
                    let (alias, plan) = derived_plan(subquery, alias.as_ref(), params)?;
                    ctes.push((alias.clone(), Box::new(plan)));
                    (alias, None)
                }
                other => anyhow::bail!("Unsupported FROM relation: {other}"),
            }
        };

    // Every relation after the first joins the ones before it: a comma-separated
    // `FROM a, b` item is a cross join, and each item's own `JOIN`s follow it.
    let mut joins = Vec::new();
    for (position, item) in from.iter().enumerate() {
        if position > 0 {
            joins.push(plan_join(
                &item.relation,
                (JoinType::Cross, None, Vec::new(), false),
                ctes,
                params,
            )?);
        }
        for j in &item.joins {
            let constraint = match &j.join_operator {
                // 0.62 distinguishes the bare keyword forms (`JOIN`, `LEFT JOIN`,
                // `RIGHT JOIN`) from the explicit `... OUTER JOIN` spellings; both
                // map to the same join type.
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
                other => {
                    let kind = format!("{other:?}");
                    let kind = kind.split('(').next().unwrap_or(&kind);
                    anyhow::bail!("{kind} joins are not supported")
                }
            };
            joins.push(plan_join(&j.relation, constraint, ctes, params)?);
        }
    }
    Ok((table_name, table_alias, joins, sample, first_only))
}

/// A FROM list as a query of all its joined rows: the relations an
/// `UPDATE ... FROM`, `DELETE ... USING`, or `MERGE ... USING` reads.
pub(crate) fn plan_relations(
    from: &[sqlparser::ast::TableWithJoins],
    params: &[Value],
) -> Result<LogicalPlan> {
    let mut ctes = Vec::new();
    let (table_name, table_alias, joins, sample, only) = plan_from(from, &mut ctes, params)?;
    Ok(LogicalPlan::Select {
        ctes,
        table_name,
        table_alias,
        joins,
        projection: Vec::new(),
        group_by: Vec::new(),
        filter: None,
        having: None,
        grouping_sets: None,
        order_by: Vec::new(),
        limit: None,
        offset: None,
        distinct: false,
        sort: Vec::new(),
        group_exprs: Vec::new(),
        distinct_on: Vec::new(),
        sample,
        only,
    })
}

/// The name of the column a grouping expression is computed into. It starts
/// with NUL, which no SQL identifier contains, so it never shadows a column.
fn group_column_name(index: usize) -> String {
    format!("\u{0}group{index}")
}

/// The value a select item computes, as an expression over the input row (or
/// the group); `None` for window calls, subqueries, and catalog placeholders.
fn item_expr(item: &ProjectionItem) -> Option<ScalarExpr> {
    match item {
        ProjectionItem::Column(c) | ProjectionItem::AliasedColumn(c, _) => {
            Some(ScalarExpr::Column(c.clone()))
        }
        ProjectionItem::Literal(v) | ProjectionItem::AliasedLiteral(v, _) => {
            Some(ScalarExpr::Literal(v.clone()))
        }
        ProjectionItem::Aggregate(op, arg) => Some(ScalarExpr::Aggregate {
            op: op.clone(),
            arg: arg.clone(),
            arg_expr: None,
            distinct: false,
            extra_args: Vec::new(),
            filter: None,
            order_by: Vec::new(),
        }),
        ProjectionItem::Expr { expr, .. } => Some(expr.clone()),
        _ => None,
    }
}

/// A select item's `AS` name, if it has one.
fn item_alias(item: &ProjectionItem) -> Option<&str> {
    match item {
        ProjectionItem::AliasedColumn(_, a) | ProjectionItem::AliasedLiteral(_, a) => Some(a),
        ProjectionItem::Expr { alias, .. }
        | ProjectionItem::Subquery { alias, .. }
        | ProjectionItem::WindowFunction { alias, .. }
        | ProjectionItem::ScalarFunction { alias, .. } => alias.as_deref(),
        _ => None,
    }
}

/// A positive integer literal (`GROUP BY 2`, `ORDER BY 1`): the 1-based
/// position of a select item. Any other constant is rejected, as PostgreSQL
/// does, rather than grouping or sorting by a constant.
fn select_position(expr: &sqlparser::ast::Expr, clause: &str) -> Result<Option<usize>> {
    use sqlparser::ast::{Expr, Value as V};
    let Expr::Value(value) = expr else {
        return Ok(None);
    };
    match &value.value {
        V::Number(n, _) => match n.parse::<usize>() {
            Ok(position) if position >= 1 => Ok(Some(position)),
            _ => anyhow::bail!("{clause} position {n} is not in select list"),
        },
        V::Placeholder(_) => Ok(None),
        _ => anyhow::bail!("non-integer constant in {clause}"),
    }
}

/// Plans `GROUP BY`: plain columns by name, and select-list positions, output
/// aliases, and expressions as computed grouping columns. Returns the grouping
/// column names, the `ROLLUP`/`CUBE`/`GROUPING SETS` expansion, and the
/// computed grouping columns.
#[allow(clippy::type_complexity)]
fn plan_group_by(
    group_by: &sqlparser::ast::GroupByExpr,
    projection: &[ProjectionItem],
    params: &[Value],
) -> Result<(
    Vec<String>,
    Option<Vec<Vec<String>>>,
    Vec<(String, ScalarExpr)>,
)> {
    use sqlparser::ast::{Expr, GroupByExpr};
    let GroupByExpr::Expressions(exprs, modifiers) = group_by else {
        anyhow::bail!("GROUP BY ALL is not supported");
    };
    if !modifiers.is_empty() {
        anyhow::bail!("Unsupported GROUP BY modifier");
    }
    let mut group_exprs: Vec<(String, ScalarExpr)> = Vec::new();
    // Plans one grouping element into grouping column names.
    let mut plan_key = |expr: &Expr, keys: &mut Vec<String>| -> Result<()> {
        let mut computed = |expr: ScalarExpr, keys: &mut Vec<String>| -> Result<()> {
            if scalar_has_aggregate(&expr) {
                anyhow::bail!("aggregate functions are not allowed in GROUP BY");
            }
            if scalar_has_window(&expr) {
                anyhow::bail!("window functions are not allowed in GROUP BY");
            }
            let name = group_column_name(group_exprs.len());
            group_exprs.push((name.clone(), expr));
            keys.push(name);
            Ok(())
        };
        if let Some(position) = select_position(expr, "GROUP BY")? {
            let item = projection.get(position - 1).ok_or_else(|| {
                anyhow::anyhow!("GROUP BY position {position} is not in select list")
            })?;
            return match item {
                ProjectionItem::Column(c) | ProjectionItem::AliasedColumn(c, _) => {
                    keys.push(c.clone());
                    Ok(())
                }
                _ => match item_expr(item) {
                    Some(expr) => computed(expr, keys),
                    None => anyhow::bail!(
                        "GROUP BY position {position} refers to an unsupported select item"
                    ),
                },
            };
        }
        match lower_scalar(expr, params) {
            Some(ScalarExpr::Column(name)) => {
                // A name that is not an input column may name an output column
                // (`SELECT upper(s) AS u ... GROUP BY u`).
                if !name.contains('.')
                    && let Some(item) = projection.iter().find(|i| item_alias(i) == Some(&name))
                {
                    match item_expr(item) {
                        Some(expr) if scalar_has_aggregate(&expr) => {}
                        Some(expr) => group_exprs.push((name.clone(), expr)),
                        None => {}
                    }
                }
                keys.push(name);
                Ok(())
            }
            Some(ScalarExpr::Row(items)) => {
                // `GROUP BY (a, b)` groups by each element; `()` by nothing.
                for item in items {
                    match item {
                        ScalarExpr::Column(name) => keys.push(name),
                        other => computed(other, keys)?,
                    }
                }
                Ok(())
            }
            Some(other) => computed(other, keys),
            None => Err(expression_error(expr, || {
                format!("Unsupported GROUP BY expression: {expr}")
            })),
        }
    };
    // Every element of the GROUP BY is a list of grouping sets; the query's
    // sets are their cross product (`GROUP BY a, ROLLUP (b)` is
    // `GROUPING SETS ((a, b), (a))`).
    let mut sets: Vec<Vec<String>> = vec![Vec::new()];
    let mut uses_sets = false;
    let product = |sets: Vec<Vec<String>>, element: Vec<Vec<String>>| -> Vec<Vec<String>> {
        sets.iter()
            .flat_map(|set| {
                element.iter().map(move |e| {
                    let mut combined = set.clone();
                    combined.extend(e.iter().cloned());
                    combined
                })
            })
            .collect()
    };
    for expr in exprs {
        let element: Vec<Vec<String>> = match expr {
            // `ROLLUP(e1, e2, …)` → prefixes: {e1..en}, …, {e1}, {}.
            Expr::Rollup(elements) => {
                let mut elems = Vec::new();
                for e in elements {
                    let mut keys = Vec::new();
                    for part in e {
                        plan_key(part, &mut keys)?;
                    }
                    elems.push(keys);
                }
                (0..=elems.len())
                    .rev()
                    .map(|i| elems[..i].concat())
                    .collect()
            }
            // `CUBE(e1, …, en)` → every subset of the elements.
            Expr::Cube(elements) => {
                let mut elems = Vec::new();
                for e in elements {
                    let mut keys = Vec::new();
                    for part in e {
                        plan_key(part, &mut keys)?;
                    }
                    elems.push(keys);
                }
                (0..(1u32 << elems.len()))
                    .rev()
                    .map(|mask| {
                        elems
                            .iter()
                            .enumerate()
                            .filter(|(bit, _)| mask & (1 << bit) != 0)
                            .flat_map(|(_, e)| e.iter().cloned())
                            .collect()
                    })
                    .collect()
            }
            // `GROUPING SETS((a,b), (a), ())` → each inner list is one set.
            Expr::GroupingSets(list) => {
                let mut element = Vec::new();
                for e in list {
                    let mut keys = Vec::new();
                    for part in e {
                        plan_key(part, &mut keys)?;
                    }
                    element.push(keys);
                }
                element
            }
            _ => {
                let mut keys = Vec::new();
                plan_key(expr, &mut keys)?;
                sets = product(sets, vec![keys]);
                continue;
            }
        };
        uses_sets = true;
        sets = product(sets, element);
    }
    // `group_by` carries the union of every column mentioned (in first
    // appearance) so the output/NULL-rollup logic can resolve them.
    let mut group_by: Vec<String> = Vec::new();
    for set in &sets {
        for c in set {
            if !group_by.contains(c) {
                group_by.push(c.clone());
            }
        }
    }
    let grouping_sets = uses_sets.then_some(sets);
    Ok((group_by, grouping_sets, group_exprs))
}

/// Plans `ORDER BY` keys. A key naming a select item by position, or
/// repeating a select item's expression, sorts by that output column; a bare
/// name is resolved when the statement runs (output column first, then input
/// column); any other expression is computed per row (or group).
fn plan_sort(
    order_by: Option<&sqlparser::ast::OrderBy>,
    projection: &[ProjectionItem],
    distinct: bool,
    params: &[Value],
) -> Result<Vec<SortKey>> {
    use sqlparser::ast::OrderByKind;
    let Some(order_by) = order_by else {
        return Ok(Vec::new());
    };
    let OrderByKind::Expressions(exprs) = &order_by.kind else {
        anyhow::bail!("ORDER BY ALL is not supported");
    };
    if order_by.interpolate.is_some() {
        anyhow::bail!("Unsupported ORDER BY INTERPOLATE");
    }
    let mut keys = Vec::with_capacity(exprs.len());
    for o in exprs {
        if o.with_fill.is_some() {
            anyhow::bail!("Unsupported ORDER BY WITH FILL");
        }
        let target = sort_target(&o.expr, projection, params)?;
        if distinct {
            let selected = match &target {
                SortTarget::Output(_) => true,
                SortTarget::Name(name) => {
                    projection.is_empty()
                        || matches!(canonical_target(&target, projection), SortTarget::Output(_))
                        || projection.iter().any(|item| {
                            matches!(item, ProjectionItem::Column(c) if c.rsplit('.').next() == Some(name))
                        })
                }
                SortTarget::Expr(_) => false,
            };
            if !selected {
                anyhow::bail!(
                    "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
                );
            }
        }
        keys.push(SortKey {
            target,
            ascending: sort_ascending(&o.options)?,
            nulls_first: o.options.nulls_first,
            with_ties: false,
        });
    }
    Ok(keys)
}

/// Resolves an `ORDER BY` or `DISTINCT ON` key: a select-list position, a
/// repeat of a select item's expression (that output column), a bare or
/// qualified name, or an expression computed per row (or group).
fn sort_target(
    expr: &sqlparser::ast::Expr,
    projection: &[ProjectionItem],
    params: &[Value],
) -> Result<SortTarget> {
    if let Some(position) = select_position(expr, "ORDER BY")? {
        // A `*` in the list stands for columns not yet known.
        let wildcard = projection
            .iter()
            .any(|p| matches!(p, ProjectionItem::Column(c) if c.ends_with('*')));
        if !projection.is_empty() && !wildcard && position > projection.len() {
            anyhow::bail!("ORDER BY position {position} is not in select list");
        }
        return Ok(SortTarget::Output(position - 1));
    }
    match lower_scalar(expr, params) {
        Some(ScalarExpr::Column(name)) => Ok(SortTarget::Name(name)),
        Some(expr) => Ok(
            match projection
                .iter()
                .position(|item| item_expr(item).as_ref() == Some(&expr))
            {
                Some(i) => SortTarget::Output(i),
                None => SortTarget::Expr(expr),
            },
        ),
        None => Err(expression_error(expr, || {
            format!("Unsupported ORDER BY expression: {expr}")
        })),
    }
}

/// A key in the form used to compare keys: a name that a select item
/// outputs (by alias or as its column) becomes that output position.
fn canonical_target(target: &SortTarget, projection: &[ProjectionItem]) -> SortTarget {
    if let SortTarget::Name(name) = target
        && let Some(i) = projection.iter().position(|item| {
            item_alias(item) == Some(name.as_str())
                || matches!(item, ProjectionItem::Column(c) if c == name)
        })
    {
        return SortTarget::Output(i);
    }
    target.clone()
}

/// Plans `LIMIT`/`OFFSET` and `FETCH FIRST n ROWS ONLY`. A NULL count means no
/// limit (or no offset); a negative or non-integer count is an error. A
/// placeholder not yet bound (while describing) leaves the clause open.
fn plan_limit(
    query: &sqlparser::ast::Query,
    params: &[Value],
    sort: &mut [SortKey],
) -> Result<(Option<usize>, Option<usize>)> {
    use sqlparser::ast::LimitClause;
    let count = |expr: &sqlparser::ast::Expr, clause: &str| -> Result<Option<usize>> {
        let value = match lower_scalar(expr, params) {
            Some(scalar) if first_column_reference(&scalar).is_none() => {
                let value = eval_scalar_expr(&scalar, &[], &[]);
                crate::eval_error::check()?;
                value
            }
            _ => anyhow::bail!("argument of {clause} must not contain variables"),
        };
        let n = match value {
            Value::Null => return Ok(None),
            Value::Int(n) => n,
            Value::Float(f) if f.is_finite() => f.round_ties_even() as i64,
            Value::Text(ref t) => t
                .trim()
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("invalid input syntax for type bigint: \"{t}\""))?,
            _ => anyhow::bail!("argument of {clause} must be type bigint"),
        };
        if n < 0 {
            anyhow::bail!("{clause} must not be negative");
        }
        Ok(Some(n as usize))
    };
    let (limit_expr, offset_expr) = match &query.limit_clause {
        Some(LimitClause::LimitOffset { limit, offset, .. }) => {
            (limit.as_ref(), offset.as_ref().map(|o| &o.value))
        }
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => (Some(limit), Some(offset)),
        None => (None, None),
    };
    let mut limit = limit_expr.map(|e| count(e, "LIMIT")).transpose()?.flatten();
    if let Some(fetch) = &query.fetch {
        if fetch.percent {
            anyhow::bail!("FETCH ... PERCENT is not supported");
        }
        if fetch.with_ties {
            if sort.is_empty() {
                anyhow::bail!("WITH TIES cannot be specified without ORDER BY clause");
            }
            for key in sort.iter_mut() {
                key.with_ties = true;
            }
        }
        if limit_expr.is_some() {
            anyhow::bail!("LIMIT and FETCH cannot both be used");
        }
        limit = match &fetch.quantity {
            Some(e) => count(e, "FETCH")?,
            None => Some(1),
        };
    }
    let offset = offset_expr
        .map(|e| count(e, "OFFSET"))
        .transpose()?
        .flatten();
    Ok((limit, offset))
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
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, TableFactor};
    match factor {
        TableFactor::Table {
            name,
            args: Some(table_args),
            with_ordinality,
            alias,
            ..
        } => {
            let exprs: Vec<&sqlparser::ast::Expr> = table_args
                .args
                .iter()
                .map(|a| match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None,
                })
                .collect::<Option<_>>()?;
            if name
                .to_string()
                .trim_start_matches("pg_catalog.")
                .eq_ignore_ascii_case(JSON_TABLE_MARKER)
            {
                return json_table_factor(&exprs, params, *with_ordinality, alias.as_ref());
            }
            if name
                .to_string()
                .trim_start_matches("pg_catalog.")
                .eq_ignore_ascii_case(XML_TABLE_MARKER)
            {
                return xml_table_factor(&exprs, params, *with_ordinality, alias.as_ref());
            }
            let mut spec = build_table_fn_spec(
                name.to_string().to_lowercase(),
                Vec::new(),
                *with_ordinality,
                alias.as_ref(),
            );
            set_table_fn_args(&mut spec, &exprs, params)?;
            Some(spec)
        }
        // `LATERAL f(...)`: its arguments may read the preceding relations.
        TableFactor::Function {
            name,
            args,
            with_ordinality,
            alias,
            ..
        } => {
            let exprs: Vec<&sqlparser::ast::Expr> = args
                .iter()
                .map(|a| match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None,
                })
                .collect::<Option<_>>()?;
            if name
                .to_string()
                .trim_start_matches("pg_catalog.")
                .eq_ignore_ascii_case(JSON_TABLE_MARKER)
            {
                return json_table_factor(&exprs, params, *with_ordinality, alias.as_ref());
            }
            if name
                .to_string()
                .trim_start_matches("pg_catalog.")
                .eq_ignore_ascii_case(XML_TABLE_MARKER)
            {
                return xml_table_factor(&exprs, params, *with_ordinality, alias.as_ref());
            }
            let mut spec = build_table_fn_spec(
                name.to_string().to_lowercase(),
                Vec::new(),
                *with_ordinality,
                alias.as_ref(),
            );
            set_table_fn_args(&mut spec, &exprs, params)?;
            Some(spec)
        }
        TableFactor::UNNEST {
            array_exprs,
            with_ordinality,
            alias,
            ..
        } => {
            let mut spec = build_table_fn_spec(
                "unnest".to_string(),
                Vec::new(),
                *with_ordinality,
                alias.as_ref(),
            );
            set_table_fn_args(&mut spec, &array_exprs.iter().collect::<Vec<_>>(), params)?;
            Some(spec)
        }
        _ => None,
    }
}

/// The first column name of a `JSON_TABLE` call that appears more than once.
fn duplicate_column_name(columns: &[JsonTableColumn], names: &mut Vec<String>) -> Option<String> {
    for column in columns {
        if let JsonTableColumnKind::Nested { columns, .. } = &column.kind {
            if let Some(name) = duplicate_column_name(columns, names) {
                return Some(name);
            }
            continue;
        }
        if names.contains(&column.name) {
            return Some(column.name.clone());
        }
        names.push(column.name.clone());
    }
    None
}

/// The `JSON_TABLE` marker, as `nodus_sql` rewrites the call:
/// `__json_table__(context, path, on-error, vars, column...)`.
const JSON_TABLE_MARKER: &str = "__json_table__";

/// The marker of one `JSON_TABLE` column; the shapes are in
/// `nodus_sql::rewrite_json_table`.
const JSON_TABLE_COLUMN_MARKERS: &[&str] = &[
    "__jt_col_ordinality__",
    "__jt_col_exists__",
    "__jt_col_scalar__",
    "__jt_nested__",
];

/// Plans `JSON_TABLE(...)` from its rewritten call: the context, the row
/// path, the `PASSING` variables, and the column markers.
fn json_table_factor(
    exprs: &[&sqlparser::ast::Expr],
    params: &[Value],
    with_ordinality: bool,
    alias: Option<&sqlparser::ast::TableAlias>,
) -> Option<TableFnSpec> {
    if exprs.len() < 4 {
        return None;
    }
    // The context item, with its `FORMAT JSON` clause as the marker the
    // elements take — the runtime reads (and checks) the item through it —
    // and the document read as `jsonb`.
    let mut doc = lower_scalar(exprs[0], params)?;
    let json_like = matches!(doc, ScalarExpr::Literal(Value::Json(_) | Value::Jsonb(_)))
        || matches!(&doc, ScalarExpr::Cast { target, .. }
            if target.eq_ignore_ascii_case("json") || crate::value::is_jsonb_type(target));
    let mut refuse = None;
    // A literal that is neither a string nor JSON is not a document, as
    // PostgreSQL says when it reads one.
    if !json_like
        && let ScalarExpr::Literal(value) = &doc
        && !matches!(value, Value::Text(_) | Value::Null)
    {
        refuse = Some(
            crate::error_fields::DbError::new(format!(
                "cannot cast type {} to jsonb",
                crate::value::value_type_name(value)
            ))
            .code("42846")
            .into_text(),
        );
    }
    if !json_like {
        doc = ScalarExpr::Cast {
            expr: Box::new(doc),
            target: "jsonb".to_string(),
        };
    }
    let path = json_table_path(exprs[1], params, &mut refuse)?;
    let on_error = marker_text(exprs[2])?;
    // The `PASSING` variables, or a NULL placeholder.
    let vars = lower_scalar(exprs[3], params)?;
    let columns = json_table_columns(&exprs[4..], params, &mut refuse)?;
    // A column name appears once, as PostgreSQL checks when it parses the
    // call.
    let mut names = Vec::new();
    if let Some(name) = duplicate_column_name(&columns, &mut names)
        && refuse.is_none()
    {
        refuse = Some(
            crate::error_fields::DbError::new(format!(
                "duplicate JSON_TABLE column or path name: {name}"
            ))
            .code("42712")
            .into_text(),
        );
    }
    let mut spec =
        build_table_fn_spec("json_table".to_string(), Vec::new(), with_ordinality, alias);
    spec.arg_exprs = vec![
        doc,
        ScalarExpr::Cast {
            expr: Box::new(vars),
            target: "jsonb".to_string(),
        },
    ];
    spec.json_table = Some(JsonTableSpec {
        path: ScalarExpr::Cast {
            expr: Box::new(path),
            target: "jsonpath".to_string(),
        },
        on_error,
        columns,
        refuse,
    });
    Some(spec)
}

/// The `XMLTABLE` marker, as `nodus_sql` rewrites the call:
/// `__xml_table__(document, path, refuse, namespace..., column...)`.
const XML_TABLE_MARKER: &str = "__xml_table__";

/// The markers of an `XMLTABLE`'s namespaces and columns; the shapes are in
/// `nodus_sql::rewrite_xmltable`.
const XML_TABLE_NAMESPACE_MARKER: &str = "__xt_ns__";
const XML_TABLE_COLUMN_MARKERS: &[&str] = &["__xt_col_ordinality__", "__xt_col__"];

/// Plans `XMLTABLE(...)` from its rewritten call: the document, the row
/// path, the `XMLNAMESPACES` items, and the column markers.
fn xml_table_factor(
    exprs: &[&sqlparser::ast::Expr],
    params: &[Value],
    with_ordinality: bool,
    alias: Option<&sqlparser::ast::TableAlias>,
) -> Option<TableFnSpec> {
    if exprs.len() < 4 {
        return None;
    }
    let refuse_now = |refuse: &mut Option<String>, message: String, code: &str| {
        if refuse.is_none() {
            *refuse = Some(
                crate::error_fields::DbError::new(message)
                    .code(code)
                    .into_text(),
            );
        }
    };
    let mut refuse = None;
    // The document, which PostgreSQL only implicitly casts to xml: a
    // literal of another type is not one.
    let doc = lower_scalar(exprs[0], params)?;
    match &doc {
        ScalarExpr::Literal(value) if !matches!(value, Value::Text(_) | Value::Null) => {
            refuse_now(
                &mut refuse,
                format!(
                    "argument of XMLTABLE must be type xml, not type {}",
                    crate::value::value_type_name(value)
                ),
                "42804",
            );
        }
        ScalarExpr::Cast { target, .. } if !crate::xml::is_type(target) => {
            refuse_now(
                &mut refuse,
                format!("argument of XMLTABLE must be type xml, not type {target}"),
                "42804",
            );
        }
        _ => {}
    }
    let path = lower_scalar(exprs[1], params)?;
    // The `refuse` string the rewriter carries: a clause PostgreSQL refuses
    // when its grammar or parse analysis reads the call.
    let rewriter_refuse = marker_text(exprs[2])?;
    if !rewriter_refuse.is_empty() && refuse.is_none() {
        refuse = Some(
            crate::error_fields::DbError::new(rewriter_refuse)
                .code("42601")
                .into_text(),
        );
    }
    let mut namespaces = Vec::new();
    let mut columns = Vec::new();
    for expr in &exprs[3..] {
        let sqlparser::ast::Expr::Function(function) = expr else {
            return None;
        };
        let name = function
            .name
            .to_string()
            .to_ascii_uppercase()
            .trim_start_matches("PG_CATALOG.")
            .to_string();
        let sqlparser::ast::FunctionArguments::List(list) = &function.args else {
            return None;
        };
        let args: Vec<&sqlparser::ast::Expr> = list
            .args
            .iter()
            .map(|a| match a {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                    Some(e)
                }
                _ => None,
            })
            .collect::<Option<_>>()?;
        let marker = name.to_ascii_lowercase();
        if marker == XML_TABLE_NAMESPACE_MARKER {
            let [prefix, uri] = args.as_slice() else {
                return None;
            };
            let prefix = match *prefix {
                sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: sqlparser::ast::Value::Null,
                    ..
                }) => None,
                other => Some(marker_text(other)?),
            };
            namespaces.push(XmlTableNamespace {
                prefix,
                uri: lower_scalar(uri, params)?,
            });
            continue;
        }
        if !XML_TABLE_COLUMN_MARKERS.contains(&marker.as_str()) {
            return None;
        }
        if marker == "__xt_col_ordinality__" {
            let [column_name] = args.as_slice() else {
                return None;
            };
            columns.push(XmlTableColumn {
                name: marker_text(column_name)?,
                kind: XmlTableColumnKind::Ordinality,
            });
            continue;
        }
        let [column_name, column_type, path, default, not_null] = args.as_slice() else {
            return None;
        };
        let column_type = marker_text(column_type)?;
        let path = marker_expression(path, params)?;
        // A `DEFAULT` literal is coerced to the column's type now, as
        // PostgreSQL coerces it when it parses the call; a literal it
        // cannot take is refused there.
        let mut default = marker_expression(default, params)?;
        if let Some(expr) = &default
            && let ScalarExpr::Literal(value) = expr
            && !matches!(value, Value::Text(_) | Value::Null)
        {
            // PostgreSQL coerces the default to the column's type with an
            // assignment cast when it parses the call; a literal with no
            // such cast is refused then.
            match xmltable_default_cast(value, &column_type) {
                Some(converted) => default = Some(ScalarExpr::Literal(converted)),
                None => refuse_now(
                    &mut refuse,
                    format!(
                        "argument of XMLTABLE must be type {column_type}, not type {}",
                        crate::value::value_type_name(value)
                    ),
                    "42804",
                ),
            }
        }
        columns.push(XmlTableColumn {
            name: marker_text(column_name)?,
            kind: XmlTableColumnKind::Value {
                column_type,
                path,
                default,
                not_null: matches!(
                    not_null,
                    sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                        value: sqlparser::ast::Value::Boolean(true),
                        ..
                    })
                ),
            },
        });
    }
    // One column per name, and one FOR ORDINALITY column, as PostgreSQL
    // checks when it parses the call.
    if refuse.is_none() {
        let mut names = Vec::new();
        let mut ordinality = false;
        for column in &columns {
            if matches!(column.kind, XmlTableColumnKind::Ordinality) {
                if ordinality {
                    refuse = Some(
                        crate::error_fields::DbError::new(
                            "only one FOR ORDINALITY column is allowed",
                        )
                        .code("42601")
                        .into_text(),
                    );
                    break;
                }
                ordinality = true;
            }
            if names.contains(&column.name) {
                refuse = Some(
                    crate::error_fields::DbError::new(format!(
                        "column name \"{}\" is not unique",
                        column.name
                    ))
                    .code("42601")
                    .into_text(),
                );
                break;
            }
            names.push(column.name.clone());
        }
    }
    // The namespaces are checked after the columns, as PostgreSQL's parse
    // analysis reads them.
    if refuse.is_none() {
        let mut seen = Vec::new();
        let mut default_seen = false;
        for namespace in &namespaces {
            match &namespace.prefix {
                Some(prefix) => {
                    if seen.contains(prefix) {
                        refuse = Some(
                            crate::error_fields::DbError::new(format!(
                                "namespace name \"{prefix}\" is not unique"
                            ))
                            .code("42601")
                            .into_text(),
                        );
                        break;
                    }
                    seen.push(prefix.clone());
                }
                None => {
                    if default_seen {
                        refuse = Some(
                            crate::error_fields::DbError::new(
                                "only one default namespace is allowed",
                            )
                            .code("42601")
                            .into_text(),
                        );
                        break;
                    }
                    default_seen = true;
                }
            }
        }
    }
    let mut spec = build_table_fn_spec("xml_table".to_string(), Vec::new(), with_ordinality, alias);
    spec.arg_exprs = vec![doc];
    spec.xml_table = Some(XmlTableSpec {
        path,
        namespaces,
        columns,
        refuse,
    });
    Some(spec)
}

/// A `DEFAULT` literal as PostgreSQL's assignment cast takes it: a number
/// goes to a numeric or character type and a boolean to a character one;
/// anything else has no cast, and the column's default is refused.
fn xmltable_default_cast(value: &Value, column_type: &str) -> Option<Value> {
    let upper = column_type.trim().to_ascii_uppercase();
    let base = upper.split('(').next().unwrap_or_default().trim();
    let character = matches!(
        base,
        "TEXT" | "VARCHAR" | "CHARACTER VARYING" | "CHAR" | "CHARACTER" | "BPCHAR" | "NAME"
    );
    let numeric = matches!(
        base,
        "SMALLINT"
            | "INT2"
            | "INTEGER"
            | "INT"
            | "INT4"
            | "BIGINT"
            | "INT8"
            | "REAL"
            | "FLOAT4"
            | "DOUBLE PRECISION"
            | "FLOAT8"
            | "FLOAT"
            | "NUMERIC"
            | "DECIMAL"
            | "OID"
            | "MONEY"
    );
    match value {
        Value::Int(_) | Value::Float(_) | Value::Numeric(_) if numeric => {
            crate::planner::try_cast(value.clone(), column_type).ok()
        }
        Value::Int(_) | Value::Float(_) | Value::Numeric(_) if character => {
            crate::value::fit_character(&crate::render(value), column_type, false)
                .ok()
                .map(Value::Text)
        }
        // A boolean's text is `true`/`false`.
        Value::Bool(flag) if character => {
            crate::value::fit_character(if *flag { "true" } else { "false" }, column_type, false)
                .ok()
                .map(Value::Text)
        }
        _ => None,
    }
}

/// The SQL-to-XML mapping functions — `table_to_xml`, `query_to_xml`,
/// `cursor_to_xml`, `schema_to_xml`, `database_to_xml`, and their
/// `*_xmlschema` and `*_and_xmlschema` forms — as the one-row plan the call
/// is the value of; `None` for any other expression.
pub(crate) fn xml_mapping_spec(
    expr: &sqlparser::ast::Expr,
    params: &[Value],
) -> Option<TableFnSpec> {
    use crate::plan_types::{XmlMappingForm, XmlMappingKind, XmlMappingSpec};
    let sqlparser::ast::Expr::Function(function) = expr else {
        return None;
    };
    let name = function
        .name
        .to_string()
        .to_ascii_uppercase()
        .trim_start_matches("PG_CATALOG.")
        .to_string();
    let (source, form) = match name.as_str() {
        "TABLE_TO_XML" => ("table", XmlMappingForm::Data),
        "TABLE_TO_XMLSCHEMA" => ("table", XmlMappingForm::Schema),
        "TABLE_TO_XML_AND_XMLSCHEMA" => ("table", XmlMappingForm::AndSchema),
        "QUERY_TO_XML" => ("query", XmlMappingForm::Data),
        "QUERY_TO_XMLSCHEMA" => ("query", XmlMappingForm::Schema),
        "QUERY_TO_XML_AND_XMLSCHEMA" => ("query", XmlMappingForm::AndSchema),
        "CURSOR_TO_XML" => ("cursor", XmlMappingForm::Data),
        "CURSOR_TO_XMLSCHEMA" => ("cursor", XmlMappingForm::Schema),
        "SCHEMA_TO_XML" => ("schema", XmlMappingForm::Data),
        "SCHEMA_TO_XMLSCHEMA" => ("schema", XmlMappingForm::Schema),
        "SCHEMA_TO_XML_AND_XMLSCHEMA" => ("schema", XmlMappingForm::AndSchema),
        "DATABASE_TO_XML" => ("database", XmlMappingForm::Data),
        "DATABASE_TO_XMLSCHEMA" => ("database", XmlMappingForm::Schema),
        "DATABASE_TO_XML_AND_XMLSCHEMA" => ("database", XmlMappingForm::AndSchema),
        _ => return None,
    };
    let sqlparser::ast::FunctionArguments::List(list) = &function.args else {
        return None;
    };
    let args: Vec<&sqlparser::ast::Expr> = list
        .args
        .iter()
        .map(|a| match a {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                Some(e)
            }
            _ => None,
        })
        .collect::<Option<_>>()?;
    // The shape each form takes: the source, then the flags; a cursor's
    // data form takes its row count between them (its schema form has
    // none).
    let cursor_count = source == "cursor" && form == XmlMappingForm::Data;
    let expected = match source {
        "database" => 3,
        "cursor" if cursor_count => 5,
        _ => 4,
    };
    if args.len() != expected {
        return None;
    }
    let flag = |e: &sqlparser::ast::Expr| lower_scalar(e, params);
    let kind = match source {
        "table" => XmlMappingKind::Table(flag(args[0])?),
        "query" => XmlMappingKind::Query(flag(args[0])?),
        "schema" => XmlMappingKind::Schema(flag(args[0])?),
        "database" => XmlMappingKind::Database,
        _ if cursor_count => XmlMappingKind::Cursor {
            name: flag(args[0])?,
            count: flag(args[1])?,
        },
        _ => XmlMappingKind::Cursor {
            name: flag(args[0])?,
            count: ScalarExpr::Literal(Value::Null),
        },
    };
    let base = match source {
        "database" => 0,
        "cursor" if cursor_count => 2,
        _ => 1,
    };
    let function = name.to_ascii_lowercase();
    let mut spec = build_table_fn_spec(function.clone(), Vec::new(), false, None);
    spec.xml_mapping = Some(XmlMappingSpec {
        function,
        kind,
        nulls: flag(args[base])?,
        tableforest: flag(args[base + 1])?,
        targetns: flag(args[base + 2])?,
        form,
    });
    Some(spec)
}

/// The large-object functions — `lo_create`, `lo_from_bytea`, `lo_get`,
/// `lo_put`, `lo_unlink`, the `lo_open` descriptor family, and
/// `loread`/`lowrite` — as the one-row plan the call is the value of;
/// `None` for any other expression.
pub(crate) fn large_object_spec(
    expr: &sqlparser::ast::Expr,
    params: &[Value],
) -> Option<TableFnSpec> {
    let sqlparser::ast::Expr::Function(function) = expr else {
        return None;
    };
    let name = function
        .name
        .to_string()
        .to_ascii_uppercase()
        .trim_start_matches("PG_CATALOG.")
        .to_string();
    // Each function's accepted arities, and the type it returns.
    let (arities, return_type): (&[usize], &str) = match name.as_str() {
        "LO_CREATE" | "LO_CREAT" => (&[1], "OID"),
        "LO_FROM_BYTEA" => (&[2], "OID"),
        "LO_GET" => (&[1, 3], "BYTEA"),
        "LO_PUT" => (&[3], "VOID"),
        "LO_UNLINK" => (&[1], "INTEGER"),
        "LO_OPEN" => (&[2], "INTEGER"),
        "LO_CLOSE" => (&[1], "INTEGER"),
        "LO_LSEEK" | "LO_LSEEK64" => (&[3], "INTEGER"),
        "LO_TELL" | "LO_TELL64" => (&[1], "INTEGER"),
        "LO_TRUNCATE" | "LO_TRUNCATE64" => (&[2], "INTEGER"),
        "LOREAD" => (&[2], "BYTEA"),
        "LOWRITE" => (&[2], "INTEGER"),
        _ => return None,
    };
    let sqlparser::ast::FunctionArguments::List(list) = &function.args else {
        return None;
    };
    let args: Vec<&sqlparser::ast::Expr> = list
        .args
        .iter()
        .map(|a| match a {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                Some(e)
            }
            _ => None,
        })
        .collect::<Option<_>>()?;
    if !arities.contains(&args.len()) {
        return None;
    }
    let lowered = args
        .iter()
        .map(|arg| lower_scalar(arg, params))
        .collect::<Option<Vec<_>>>()?;
    let function = name.to_ascii_lowercase();
    let mut spec = build_table_fn_spec(function.clone(), Vec::new(), false, None);
    spec.large_object = Some(crate::plan_types::LargeObjectSpec {
        function,
        args: lowered,
        return_type: return_type.to_string(),
    });
    Some(spec)
}

/// A marker argument that is an expression, or the NULL placeholder a
/// missing `PATH`/`DEFAULT` leaves.
fn marker_expression(expr: &sqlparser::ast::Expr, params: &[Value]) -> Option<Option<ScalarExpr>> {
    match expr {
        sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
            value: sqlparser::ast::Value::Null,
            ..
        }) => Some(None),
        other => Some(Some(lower_scalar(other, params)?)),
    }
}

/// A string literal a `JSON_TABLE` marker carries.
fn marker_text(expr: &sqlparser::ast::Expr) -> Option<String> {
    match expr {
        sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
            value: sqlparser::ast::Value::SingleQuotedString(text),
            ..
        }) => Some(text.clone()),
        _ => None,
    }
}

/// A `JSON_TABLE` path expression, as the `jsonpath` it is evaluated as.
/// PostgreSQL parses a literal path when it parses the statement, so one
/// that is not a path is refused before any row.
fn json_table_path(
    expr: &sqlparser::ast::Expr,
    params: &[Value],
    refuse: &mut Option<String>,
) -> Option<ScalarExpr> {
    if let Some(text) = marker_text(expr)
        && let Err(error) = crate::jsonpath::canonical(&text)
        && refuse.is_none()
    {
        *refuse = Some(error);
    }
    lower_scalar(expr, params)
}

/// The columns of a `JSON_TABLE` call, with the checks PostgreSQL makes when
/// it parses one. A check it fails is carried in `refuse`, and raised when
/// the table is evaluated.
fn json_table_columns(
    exprs: &[&sqlparser::ast::Expr],
    params: &[Value],
    refuse: &mut Option<String>,
) -> Option<Vec<JsonTableColumn>> {
    let mut columns = Vec::new();
    for expr in exprs {
        let sqlparser::ast::Expr::Function(function) = expr else {
            return None;
        };
        let name = function
            .name
            .to_string()
            .to_ascii_uppercase()
            .trim_start_matches("PG_CATALOG.")
            .to_string();
        if !JSON_TABLE_COLUMN_MARKERS.contains(&name.to_ascii_lowercase().as_str()) {
            return None;
        }
        let sqlparser::ast::FunctionArguments::List(list) = &function.args else {
            return None;
        };
        let args: Vec<&sqlparser::ast::Expr> = list
            .args
            .iter()
            .map(|a| match a {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                    Some(e)
                }
                _ => None,
            })
            .collect::<Option<_>>()?;
        let refuse_now = |refuse: &mut Option<String>, message: String, code: &str| {
            if refuse.is_none() {
                *refuse = Some(
                    crate::error_fields::DbError::new(message)
                        .code(code)
                        .into_text(),
                );
            }
        };
        let column = match name.as_str() {
            "__JT_COL_ORDINALITY__" => {
                let [column_name] = args.as_slice() else {
                    return None;
                };
                JsonTableColumn {
                    name: marker_text(column_name)?,
                    kind: JsonTableColumnKind::Ordinality,
                }
            }
            "__JT_COL_EXISTS__" => {
                let [column_name, column_type, path, on_error] = args.as_slice() else {
                    return None;
                };
                let column_type = marker_text(column_type)?;
                let name = marker_text(column_name)?;
                let on_error = marker_text(on_error)?;
                if !matches!(
                    on_error.as_str(),
                    "" | "error" | "true" | "false" | "unknown"
                ) {
                    refuse_now(
                        refuse,
                        format!("invalid ON ERROR behavior for column \"{name}\""),
                        "42601",
                    );
                }
                JsonTableColumn {
                    name,
                    kind: JsonTableColumnKind::Exists {
                        column_type,
                        path: json_table_path(path, params, refuse)?,
                        on_error,
                    },
                }
            }
            "__JT_COL_SCALAR__" => {
                let [
                    column_name,
                    column_type,
                    format,
                    encoded,
                    path,
                    wrapper,
                    quotes,
                    on_empty,
                    on_empty_default,
                    on_error,
                    on_error_default,
                ] = args.as_slice()
                else {
                    return None;
                };
                let name = marker_text(column_name)?;
                let column_type = marker_text(column_type)?;
                let flag = |e: &sqlparser::ast::Expr| {
                    matches!(
                        e,
                        sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                            value: sqlparser::ast::Value::Boolean(true),
                            ..
                        })
                    )
                };
                let format = flag(format);
                let encoded = flag(encoded);
                if encoded && !crate::value::is_bytea_type(&column_type) {
                    refuse_now(
                        refuse,
                        "cannot set JSON encoding for non-bytea output types".to_string(),
                        "0A000",
                    );
                }
                if format && !json_table_format_type(&column_type) {
                    refuse_now(
                        refuse,
                        "cannot use JSON format with non-string output types".to_string(),
                        "0A000",
                    );
                }
                let jsonish = crate::value::is_json_type(&column_type)
                    || crate::value::is_jsonb_type(&column_type);
                for (clause, behavior) in [
                    ("ON EMPTY", marker_text(on_empty)?),
                    ("ON ERROR", marker_text(on_error)?),
                ] {
                    let allowed = matches!(behavior.as_str(), "" | "null" | "error" | "default")
                        || (jsonish && matches!(behavior.as_str(), "empty-array" | "empty-object"));
                    if !allowed {
                        refuse_now(
                            refuse,
                            format!("invalid {clause} behavior for column \"{name}\""),
                            "42601",
                        );
                    }
                }
                JsonTableColumn {
                    name,
                    kind: JsonTableColumnKind::Scalar {
                        column_type,
                        format,
                        encoded,
                        path: json_table_path(path, params, refuse)?,
                        wrapper: marker_text(wrapper)?,
                        quotes: marker_text(quotes)?,
                        on_empty: marker_text(on_empty)?,
                        on_empty_default: lower_scalar(on_empty_default, params),
                        on_error: marker_text(on_error)?,
                        on_error_default: lower_scalar(on_error_default, params),
                    },
                }
            }
            _ => {
                let [path, rest @ ..] = args.as_slice() else {
                    return None;
                };
                JsonTableColumn {
                    name: String::new(),
                    kind: JsonTableColumnKind::Nested {
                        path: json_table_path(path, params, refuse)?,
                        columns: json_table_columns(rest, params, refuse)?,
                    },
                }
            }
        };
        columns.push(column);
    }
    Some(columns)
}

/// The types a `JSON_TABLE` column's `FORMAT JSON` clause takes, besides
/// `json` and `jsonb`.
fn json_table_format_type(data_type: &str) -> bool {
    let upper = data_type.trim().to_ascii_uppercase();
    let base = upper.split('(').next().unwrap_or_default().trim();
    matches!(
        base,
        "TEXT"
            | "VARCHAR"
            | "CHARACTER VARYING"
            | "CHAR"
            | "CHARACTER"
            | "BPCHAR"
            | "NAME"
            | "BYTEA"
            | "JSON"
            | "JSONB"
    )
}

/// A table function's arguments: constants and column references as
/// operands, or, when any is an expression, all as expressions. `None` for
/// an argument that cannot be evaluated, so it is never silently dropped.
fn set_table_fn_args(
    spec: &mut TableFnSpec,
    exprs: &[&sqlparser::ast::Expr],
    params: &[Value],
) -> Option<()> {
    match exprs
        .iter()
        .map(|e| expr_to_operand(e, params))
        .collect::<Option<Vec<_>>>()
    {
        Some(args) => spec.args = args,
        None => {
            spec.arg_exprs = exprs
                .iter()
                .map(|e| lower_scalar(e, params))
                .collect::<Option<_>>()?;
        }
    }
    Some(())
}

/// The relation whose columns stand in for a select list's set-returning
/// function calls. Its name starts with NUL, so no query can spell it.
pub(crate) const SRF_RELATION: &str = "\u{0}srf";

/// Whether a function (by its upper-case name) returns a set of rows.
pub(crate) fn is_set_returning(name: &str) -> bool {
    let name = name.strip_prefix("PG_CATALOG.").unwrap_or(name);
    SELECT_LIST_TABLE_FUNCTIONS
        .iter()
        .any(|f| f.eq_ignore_ascii_case(name))
}

fn scalar_has_srf(expr: &ScalarExpr) -> bool {
    matches!(expr, ScalarExpr::Function { name, .. } if is_set_returning(name))
        || expr.children().into_iter().any(scalar_has_srf)
}

fn select_item_has_srf(item: &sqlparser::ast::SelectItem) -> bool {
    use sqlparser::ast::SelectItem;
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return false,
    };
    lower_scalar(expr, &[]).is_some_and(|e| scalar_has_srf(&e))
}

/// Replaces the set-returning function calls in the select list (and in
/// ORDER BY expressions) with columns of one table function that runs them
/// together, in lockstep; `None` when there are none.
fn lift_set_returning_functions(
    projection: &mut [ProjectionItem],
    sort: &mut [SortKey],
) -> Option<TableFnSpec> {
    fn lift(expr: &ScalarExpr, calls: &mut Vec<(ScalarExpr, String)>) -> ScalarExpr {
        if let ScalarExpr::Function { name, .. } = expr
            && is_set_returning(name)
        {
            if let Some((_, column)) = calls.iter().find(|(call, _)| call == expr) {
                return ScalarExpr::Column(column.clone());
            }
            let column = format!("{SRF_RELATION}.c{}", calls.len() + 1);
            calls.push((expr.clone(), column.clone()));
            return ScalarExpr::Column(column);
        }
        expr.map_children(&mut |e| lift(e, calls))
    }
    let mut calls = Vec::new();
    for item in projection.iter_mut() {
        if let ProjectionItem::Expr { expr, .. } = item {
            *expr = lift(expr, &mut calls);
        }
    }
    for key in sort.iter_mut() {
        if let SortTarget::Expr(e) = &mut key.target {
            *e = lift(e, &mut calls);
        }
    }
    if calls.is_empty() {
        return None;
    }
    let member = |call: &ScalarExpr| {
        let ScalarExpr::Function { name, args } = call else {
            unreachable!("only function calls are lifted");
        };
        let name = name.to_ascii_lowercase();
        TableFnSpec {
            name: name
                .strip_prefix("pg_catalog.")
                .unwrap_or(&name)
                .to_string(),
            args: Vec::new(),
            with_ordinality: false,
            alias: None,
            column_aliases: Vec::new(),
            column_types: Vec::new(),
            arg_exprs: args.clone(),
            rows_from: Vec::new(),
            json_table: None,
            xml_table: None,
            xml_mapping: None,
            large_object: None,
        }
    };
    Some(TableFnSpec {
        name: "rows from".to_string(),
        args: Vec::new(),
        with_ordinality: false,
        alias: Some(SRF_RELATION.to_string()),
        column_aliases: (1..=calls.len()).map(|i| format!("c{i}")).collect(),
        column_types: Vec::new(),
        arg_exprs: Vec::new(),
        rows_from: calls.iter().map(|(call, _)| member(call)).collect(),
        json_table: None,
        xml_table: None,
        xml_mapping: None,
        large_object: None,
    })
}

/// A `VALUES` list with each untyped string literal cast to its column's
/// type, when another row gives the column a type that is not text
/// (`VALUES ('\\x01'::bytea), ('\\x0a')` is two `bytea` values).
fn typed_values_rows(mut rows: Vec<Vec<ScalarExpr>>) -> Vec<Vec<ScalarExpr>> {
    let untyped = |e: &ScalarExpr| matches!(e, ScalarExpr::Literal(Value::Text(_) | Value::Null));
    let columns = rows.first().map_or(0, Vec::len);
    for col in 0..columns {
        let Some(ty) = rows
            .iter()
            .map(|row| &row[col])
            .find(|e| !untyped(e))
            .and_then(crate::result_types::constant_expr_type)
        else {
            continue;
        };
        let upper = ty.to_ascii_uppercase();
        let textual = ["TEXT", "VARCHAR", "CHARACTER", "CHAR", "BPCHAR", "NAME"]
            .iter()
            .any(|t| upper.starts_with(t));
        if textual {
            continue;
        }
        for row in &mut rows {
            if matches!(row[col], ScalarExpr::Literal(Value::Text(_))) {
                let literal = row[col].clone();
                row[col] = ScalarExpr::Cast {
                    expr: Box::new(literal),
                    target: ty.clone(),
                };
            }
        }
    }
    rows
}

/// The table functions a select list may call for their rows.
const SELECT_LIST_TABLE_FUNCTIONS: &[&str] = &[
    "unnest",
    "generate_series",
    "jsonb_array_elements",
    "json_array_elements",
    "jsonb_array_elements_text",
    "json_array_elements_text",
    "jsonb_path_query",
    "jsonb_path_query_tz",
    "regexp_split_to_table",
    "regexp_matches",
    "string_to_table",
    "generate_subscripts",
    "jsonb_object_keys",
    "json_object_keys",
    "pg_partition_ancestors",
];

/// A select-list item that calls a set-returning function with plain
/// arguments, as a table function named for its output column.
fn select_list_table_function(
    item: &sqlparser::ast::SelectItem,
    params: &[Value],
) -> Option<TableFnSpec> {
    use sqlparser::ast::{Expr, FunctionArguments, SelectItem};
    let (Expr::Function(function), alias) = (match item {
        SelectItem::UnnamedExpr(expr) => (expr, None),
        SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
        _ => return None,
    }) else {
        return None;
    };
    let name = function.name.to_string().to_ascii_lowercase();
    let name = name
        .strip_prefix("pg_catalog.")
        .unwrap_or(&name)
        .to_string();
    if !SELECT_LIST_TABLE_FUNCTIONS.contains(&name.as_str()) || function.over.is_some() {
        return None;
    }
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    let exprs: Vec<&Expr> = list
        .args
        .iter()
        .map(|a| match a {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                Some(e)
            }
            _ => None,
        })
        .collect::<Option<_>>()?;
    let mut spec = TableFnSpec {
        alias: Some(alias.unwrap_or_else(|| name.clone())),
        name,
        args: Vec::new(),
        with_ordinality: false,
        column_aliases: Vec::new(),
        column_types: Vec::new(),
        arg_exprs: Vec::new(),
        rows_from: Vec::new(),
        json_table: None,
        xml_table: None,
        xml_mapping: None,
        large_object: None,
    };
    set_table_fn_args(&mut spec, &exprs, params)?;
    Some(spec)
}

fn build_table_fn_spec(
    name: String,
    args: Vec<Operand>,
    with_ordinality: bool,
    alias: Option<&sqlparser::ast::TableAlias>,
) -> TableFnSpec {
    TableFnSpec {
        name: name
            .strip_prefix("pg_catalog.")
            .map(str::to_string)
            .unwrap_or(name),
        args,
        with_ordinality,
        alias: alias.map(|a| a.name.value.clone()),
        column_aliases: alias
            .map(|a| a.columns.iter().map(|c| c.name.value.clone()).collect())
            .unwrap_or_default(),
        column_types: alias
            .map(|a| {
                a.columns
                    .iter()
                    .map(|c| c.data_type.as_ref().map(|t| t.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        arg_exprs: Vec::new(),
        rows_from: Vec::new(),
        json_table: None,
        xml_table: None,
        xml_mapping: None,
        large_object: None,
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
            // The markers the XML syntax is rewritten to keep the name of the
            // construct they stand for.
            if ident.quote_style.is_none() {
                match ident.value.to_ascii_lowercase().as_str() {
                    "__xmlparse__" => return Some("xmlparse".to_string()),
                    "__xmlserialize__" => return Some("xmlserialize".to_string()),
                    "__xmlelement__" => return Some("xmlelement".to_string()),
                    "__xmlforest__" => return Some("xmlforest".to_string()),
                    "__xmlpi__" => return Some("xmlpi".to_string()),
                    "__xmlroot__" => return Some("xmlroot".to_string()),
                    "__xmlattributes__" => return None,
                    "__xml_is_document__" => return None,
                    "__json_exists__" => return Some("json_exists".to_string()),
                    "__json_value__" => return Some("json_value".to_string()),
                    "__json_query__" => return Some("json_query".to_string()),
                    "__json_vars__" => return None,
                    "__json__" => return Some("json".to_string()),
                    "__json_scalar__" => return Some("json_scalar".to_string()),
                    "__json_serialize__" => return Some("json_serialize".to_string()),
                    "__json_array__" => return Some("json_array".to_string()),
                    "__json_object__" => return Some("json_object".to_string()),
                    "__json_arrayagg__" => return Some("json_arrayagg".to_string()),
                    "__json_array_query__" => return Some("json_array".to_string()),
                    "__json_objectagg__" => return Some("json_objectagg".to_string()),
                    "__json_format__" => return None,
                    "__xmlexists__" => return Some("xmlexists".to_string()),
                    _ => {}
                }
            }
            if ident.quote_style.is_some() {
                ident.value.clone()
            } else {
                ident.value.to_ascii_lowercase()
            }
        }
        // A cast names its column by the value cast, unless that is only
        // named by a type itself (`'t'::regclass::text` is `text`).
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let mut value = &**inner;
            while let Expr::Nested(nested) = value {
                value = nested;
            }
            let typed = matches!(value, Expr::Cast { .. } | Expr::TypedString(_));
            figure_colname(inner)
                .filter(|name| name != "case" && !typed)
                .unwrap_or_else(|| cast_type_name(&data_type.to_string()))
        }
        Expr::TypedString(typed) => cast_type_name(&typed.data_type.to_string()),
        Expr::Interval(_) => "interval".to_string(),
        Expr::Case { .. } => "case".to_string(),
        Expr::Array(_) => "array".to_string(),
        Expr::Tuple(_) => "row".to_string(),
        Expr::Exists { .. } => "exists".to_string(),
        Expr::Subquery(query) => return query_output_name(query),
        Expr::Extract { .. } => "extract".to_string(),
        Expr::AtTimeZone { .. } => "timezone".to_string(),
        Expr::Position { .. } => "position".to_string(),
        Expr::Substring { shorthand, .. } => {
            if *shorthand { "substr" } else { "substring" }.to_string()
        }
        Expr::Overlay { .. } => "overlay".to_string(),
        Expr::IsNormalized { .. } => "is_normalized".to_string(),
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
        // Any other type is named by its own name, without its schema.
        _ => {
            let base = data_type
                .trim()
                .trim_end_matches("[]")
                .split('(')
                .next()
                .unwrap_or_default()
                .trim();
            let name = base.rsplit('.').next().unwrap_or(base);
            return match name.strip_prefix('"').and_then(|n| n.strip_suffix('"')) {
                Some(quoted) => quoted.to_string(),
                None => name.to_ascii_lowercase(),
            };
        }
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
        // A plain aggregate over a column keeps its dedicated item.
        Some(ScalarExpr::Aggregate {
            op,
            arg,
            arg_expr: None,
            distinct: false,
            extra_args,
            filter: None,
            order_by,
        }) if alias.is_none()
            && extra_args.is_empty()
            && order_by.is_empty()
            && matches!(
                op,
                AggregateOp::Count
                    | AggregateOp::Sum
                    | AggregateOp::Min
                    | AggregateOp::Max
                    | AggregateOp::Avg
            ) =>
        {
            Ok(ProjectionItem::Aggregate(op, arg))
        }
        Some(scalar) => Ok(ProjectionItem::Expr {
            expr: scalar,
            alias: Some(name()),
        }),
        None => {
            if let Some(message) = unknown_function_error(expr)
                && message.starts_with("MERGE_ACTION()")
            {
                anyhow::bail!(message);
            }
            // Catalog introspection calls functions NodusDB does not provide
            // over virtual tables; the executor resolves (or rejects) them.
            if let Expr::Function(func) = expr
                && func.filter.is_none()
                && aggregate_op(
                    func.name
                        .to_string()
                        .to_uppercase()
                        .trim_start_matches("PG_CATALOG."),
                )
                .is_none()
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
            Err(expression_error(expr, || {
                format!("Unsupported expression in SELECT: {expr}")
            }))
        }
    }
}

/// A relation's alias; a column alias list (`t AS x(a, b)`) on a table is not
/// supported.
fn table_alias_name(alias: Option<&sqlparser::ast::TableAlias>) -> Result<Option<String>> {
    match alias {
        Some(alias) if !alias.columns.is_empty() => {
            anyhow::bail!("column aliases on a table are not supported")
        }
        Some(alias) => Ok(Some(alias.name.value.clone())),
        None => Ok(None),
    }
}

/// Plans a derived table (`(subquery) AS alias [(columns)]`): its query,
/// renamed by any column alias list.
fn derived_plan(
    subquery: &sqlparser::ast::Query,
    alias: Option<&sqlparser::ast::TableAlias>,
    params: &[Value],
) -> Result<(String, LogicalPlan)> {
    let plan = plan_query(subquery, params)?;
    // Without an alias (allowed since PostgreSQL 16) the subquery gets a name
    // of its own that no query can spell, so two of them never collide.
    let Some(alias) = alias else {
        let name = format!("\u{0}subquery{:p}", subquery);
        return Ok((name, plan));
    };
    let plan = if alias.columns.is_empty() {
        plan
    } else {
        LogicalPlan::Renamed {
            input: Box::new(plan),
            columns: alias.columns.iter().map(|c| c.name.value.clone()).collect(),
        }
    };
    Ok((alias.name.value.clone(), plan))
}

/// Plans the relation on the right of a join, with the join's type and
/// condition. A table function or `LATERAL` subquery runs for each left row;
/// any other derived table is computed once, as a CTE.
fn plan_join(
    relation: &sqlparser::ast::TableFactor,
    (join_type, condition, using_columns, natural): (
        JoinType,
        Option<FilterExpr>,
        Vec<String>,
        bool,
    ),
    ctes: &mut Vec<(String, Box<LogicalPlan>)>,
    params: &[Value],
) -> Result<crate::Join> {
    use sqlparser::ast::TableFactor;
    let sample = factor_sample(relation, params)?;
    let join = |table_name: String, table_alias: Option<String>| crate::Join {
        only: false,
        table_name,
        table_alias,
        condition: condition.clone(),
        join_type: join_type.clone(),
        using_columns: using_columns.clone(),
        natural,
        table_fn: None,
        lateral: None,
        sample: sample.clone(),
    };
    if let Some(spec) = table_fn_from_factor(relation, params) {
        let alias = spec.alias.clone().unwrap_or_else(|| spec.name.clone());
        // A table function joins every left row (a cross join), or keeps the
        // left row with NULLs when it yields nothing (a left join).
        let join_type = match join_type {
            JoinType::LeftOuter => JoinType::LeftOuter,
            JoinType::Cross => JoinType::Cross,
            _ => JoinType::Inner,
        };
        return Ok(crate::Join {
            join_type,
            table_fn: Some(spec),
            ..join(alias.clone(), Some(alias))
        });
    }
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let (name, only) = factor_only(name);
            Ok(crate::Join {
                only,
                ..join(name, table_alias_name(alias.as_ref())?)
            })
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
            ..
        } => {
            let (alias, plan) = derived_plan(subquery, alias.as_ref(), params)?;
            if *lateral {
                if matches!(join_type, JoinType::RightOuter | JoinType::FullOuter) {
                    anyhow::bail!("RIGHT and FULL joins with a LATERAL subquery are not supported");
                }
                Ok(crate::Join {
                    lateral: Some(Box::new(plan)),
                    ..join(alias.clone(), Some(alias))
                })
            } else {
                ctes.push((alias.clone(), Box::new(plan)));
                Ok(join(alias.clone(), Some(alias)))
            }
        }
        other => anyhow::bail!("Unsupported join relation: {other}"),
    }
}

/// A query over an already-planned result (a set operation, a `VALUES` list,
/// or a SELECT without FROM): the result itself when the query adds nothing, else a select from it
/// as a CTE, which applies the query's `WITH`, `ORDER BY` (result column names
/// or positions only), and `LIMIT`.
fn select_from_result(
    result: LogicalPlan,
    mut ctes: Vec<(String, Box<LogicalPlan>)>,
    query: &sqlparser::ast::Query,
    params: &[Value],
) -> Result<LogicalPlan> {
    if ctes.is_empty()
        && query.order_by.is_none()
        && query.limit_clause.is_none()
        && query.fetch.is_none()
    {
        return Ok(result);
    }
    let mut sort = plan_sort(query.order_by.as_ref(), &[], false, params)?;
    if sort.iter().any(|k| matches!(k.target, SortTarget::Expr(_))) {
        anyhow::bail!(
            "invalid UNION/INTERSECT/EXCEPT ORDER BY clause: only result column names or positions can be used"
        );
    }
    let (limit, offset) = plan_limit(query, params, &mut sort)?;
    let name = "\u{0}result".to_string();
    ctes.push((name.clone(), Box::new(result)));
    Ok(LogicalPlan::Select {
        only: false,
        ctes,
        table_name: name,
        table_alias: None,
        joins: Vec::new(),
        projection: Vec::new(),
        group_by: Vec::new(),
        filter: None,
        having: None,
        grouping_sets: None,
        order_by: Vec::new(),
        limit,
        offset,
        distinct: false,
        sort,
        group_exprs: Vec::new(),
        distinct_on: Vec::new(),
        sample: None,
    })
}
