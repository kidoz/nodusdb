//! Subqueries inside expressions, and `VALUES` lists.
//!
//! Expression evaluation is pure, so a subquery in an expression is run here
//! first — once per row, with that row's values for its outer references —
//! and replaced by its value before the expression is evaluated.

use crate::*;
use anyhow::Result;

/// Whether an expression contains a subquery to run first.
pub(crate) fn contains_subquery(expr: &ScalarExpr) -> bool {
    matches!(expr, ScalarExpr::Subquery { .. })
        || expr.children().into_iter().any(contains_subquery)
}

impl MemExecutor {
    /// Evaluates `expr` over `row`, running any subqueries in it first.
    pub(crate) fn eval_expr(
        &self,
        ctx: &ExecutionContext,
        expr: &ScalarExpr,
        row: &[Value],
        col_names: &[String],
    ) -> Value {
        if contains_subquery(expr) {
            eval_scalar_expr(
                &self.resolve_subqueries(ctx, expr, row, col_names),
                row,
                col_names,
            )
        } else {
            eval_scalar_expr(expr, row, col_names)
        }
    }

    /// Evaluates `expr` over a group (aggregates over its rows, columns from
    /// its first row), running any subqueries in it for the first row.
    pub(crate) fn eval_grouped(
        &self,
        ctx: &ExecutionContext,
        expr: &ScalarExpr,
        group_rows: &[Vec<Value>],
        col_names: &[String],
    ) -> Value {
        if contains_subquery(expr) {
            let rep = group_rows.first().map(Vec::as_slice).unwrap_or(&[]);
            let resolved = self.resolve_subqueries(ctx, expr, rep, col_names);
            crate::aggregates::eval_scalar_expr_grouped(&resolved, group_rows, col_names)
        } else {
            crate::aggregates::eval_scalar_expr_grouped(expr, group_rows, col_names)
        }
    }

    /// `expr` with each subquery replaced by its value for `row`.
    pub(crate) fn resolve_subqueries(
        &self,
        ctx: &ExecutionContext,
        expr: &ScalarExpr,
        row: &[Value],
        col_names: &[String],
    ) -> ScalarExpr {
        match expr {
            ScalarExpr::Subquery { plan, kind } => {
                ScalarExpr::Literal(self.subquery_value(ctx, &plan.0, *kind, row, col_names))
            }
            _ if !contains_subquery(expr) => expr.clone(),
            _ => {
                expr.map_children(&mut |child| self.resolve_subqueries(ctx, child, row, col_names))
            }
        }
    }

    /// A subquery's value for one outer row. Its failures fail the statement.
    fn subquery_value(
        &self,
        ctx: &ExecutionContext,
        plan: &LogicalPlan,
        kind: SubqueryKind,
        row: &[Value],
        col_names: &[String],
    ) -> Value {
        let correlated = self.correlate_subplan(plan, row, col_names);
        let out = match self.execute_logical_inner(ctx, correlated) {
            Ok(out) => out,
            Err(e) => return crate::eval_error::raise(e.to_string()),
        };
        if kind != SubqueryKind::Exists && out.columns.len() != 1 {
            return crate::eval_error::raise("subquery must return only one column");
        }
        let mut values = out
            .rows
            .into_iter()
            .map(|r| r.values.into_iter().next().unwrap_or(Value::Null));
        match kind {
            SubqueryKind::Exists => Value::Bool(values.next().is_some()),
            SubqueryKind::Array => Value::Array(values.collect()),
            SubqueryKind::Scalar => match (values.next(), values.next()) {
                (None, _) => Value::Null,
                (Some(value), None) => value,
                (Some(_), Some(_)) => crate::eval_error::raise(
                    "more than one row returned by a subquery used as an expression",
                ),
            },
        }
    }

    /// A `VALUES` list: each row's expressions evaluated, and each column
    /// given one type across the rows, as PostgreSQL resolves it.
    pub(crate) fn exec_values(
        &self,
        ctx: &ExecutionContext,
        rows: Vec<Vec<ScalarExpr>>,
    ) -> Result<QueryOutput> {
        let width = rows.first().map_or(0, Vec::len);
        let mut values: Vec<Vec<Value>> = rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|e| self.eval_expr(ctx, e, &[], &[]))
                    .collect()
            })
            .collect();
        crate::eval_error::check()?;
        let mut types = Vec::with_capacity(width);
        for column in 0..width {
            types.push(unify_column(&mut values, column)?);
        }
        let rows_out = values
            .into_iter()
            .map(|values| Row { values })
            .collect::<Vec<_>>();
        Ok(QueryOutput {
            columns: (1..=width).map(|i| format!("column{i}")).collect(),
            types,
            tag: format!("SELECT {}", rows_out.len()),
            rows: rows_out,
        })
    }
}

/// Resolves one `VALUES` column to a single type, converting its values: text
/// literals take the column's type, integers widen to numeric or float, and
/// types that cannot be matched are an error.
fn unify_column(rows: &mut [Vec<Value>], column: usize) -> Result<String> {
    #[derive(PartialEq, PartialOrd, Clone, Copy)]
    enum Kind {
        Int,
        Numeric,
        Float,
    }
    let cells = || {
        rows.iter()
            .filter_map(|r| r.get(column))
            .filter(|v| **v != Value::Null)
    };
    let number = cells()
        .filter_map(|v| match v {
            Value::Int(_) => Some(Kind::Int),
            Value::Numeric(_) => Some(Kind::Numeric),
            Value::Float(_) => Some(Kind::Float),
            _ => None,
        })
        .fold(None, |acc: Option<Kind>, k| {
            Some(acc.map_or(k, |a| if k > a { k } else { a }))
        });
    let target = match number {
        Some(Kind::Int) => {
            let wide = cells().any(|v| matches!(v, Value::Int(i) if i32::try_from(*i).is_err()));
            if wide { "BIGINT" } else { "INTEGER" }
        }
        Some(Kind::Numeric) => "NUMERIC",
        Some(Kind::Float) => "DOUBLE PRECISION",
        None => {
            let first = cells().next().cloned();
            return Ok(match first {
                Some(Value::Bool(_)) => {
                    convert_all(rows, column, "BOOLEAN")?;
                    "BOOLEAN"
                }
                Some(Value::Jsonb(_)) => "JSONB",
                Some(Value::Array(_)) => "TEXT[]",
                _ => "TEXT",
            }
            .to_string());
        }
    };
    convert_all(rows, column, target)?;
    Ok(target.to_string())
}

fn convert_all(rows: &mut [Vec<Value>], column: usize, target: &str) -> Result<()> {
    for row in rows.iter_mut() {
        if let Some(cell) = row.get_mut(column)
            && *cell != Value::Null
        {
            *cell = crate::planner::try_cast(cell.clone(), target)
                .map_err(|e| anyhow::anyhow!("VALUES types cannot be matched: {e}"))?;
        }
    }
    Ok(())
}
