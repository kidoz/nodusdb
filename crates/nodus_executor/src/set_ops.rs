//! Set operations (UNION/INTERSECT/EXCEPT) and literal SELECTs: combining child
//! query results by multiset semantics, and projecting constant rows.

use crate::*;
use anyhow::Result;

impl MemExecutor {
    pub(crate) fn exec_select_literal(
        &self,
        ctx: &ExecutionContext,
        values: Vec<(String, Value, Option<String>)>,
        filter: Option<FilterExpr>,
        deferred: Vec<Option<DeferredItem>>,
    ) -> Result<QueryOutput> {
        let mut columns = Vec::new();
        let mut types = Vec::new();
        let mut row_values = Vec::new();

        let mut deferred = deferred.into_iter();
        for (alias, value, type_hint) in values {
            let mut type_hint = type_hint;
            let value = match deferred.next().flatten() {
                Some(DeferredItem::Scalar(expr)) => {
                    type_hint =
                        type_hint.or_else(|| crate::result_types::constant_expr_type(&expr));
                    let value = eval_scalar_expr(&expr, &[], &[]);
                    crate::eval_error::check()?;
                    value
                }
                Some(DeferredItem::Subquery(plan)) => self.scalar_subquery_value(ctx, *plan)?,
                Some(DeferredItem::Exists { plan, negated }) => {
                    let found = !self.execute_logical_inner(ctx, *plan)?.rows.is_empty();
                    Value::Bool(found != negated)
                }
                None => value,
            };
            columns.push(alias);
            // A cast's or function's declared type wins (so `NULL::int` is
            // int4 and `now()` is timestamptz); otherwise infer from the value.
            let ty = match type_hint {
                Some(t) => t,
                None => match &value {
                    Value::Int(i) if i32::try_from(*i).is_ok() => "INTEGER".to_string(),
                    Value::Int(_) => "BIGINT".to_string(),
                    Value::Float(_) => "DOUBLE".to_string(),
                    Value::Numeric(_) => "NUMERIC".to_string(),
                    Value::Bool(_) => "BOOLEAN".to_string(),
                    Value::Jsonb(_) => "JSONB".to_string(),
                    // An untyped string literal resolves to text.
                    Value::Text(_) => "TEXT".to_string(),
                    _ => "VARCHAR".to_string(),
                },
            };
            types.push(ty);
            row_values.push(value);
        }

        // A WHERE on a FROM-less SELECT is a constant predicate (it can still
        // contain subqueries): keep the row iff it evaluates true.
        let keep = self
            .eval_filter(ctx, &[], &[], &[], filter.as_ref())
            .unwrap_or(false);
        let rows = if keep {
            vec![Row { values: row_values }]
        } else {
            Vec::new()
        };
        let tag = format!("SELECT {}", rows.len());
        Ok(QueryOutput {
            columns,
            types,
            rows,
            tag,
        })
    }

    /// Runs a scalar subquery used as a value: its single column from at most
    /// one row, NULL when it returns none.
    pub(crate) fn scalar_subquery_value(
        &self,
        ctx: &ExecutionContext,
        plan: LogicalPlan,
    ) -> Result<Value> {
        let out = self.execute_logical_inner(ctx, plan)?;
        if out.columns.len() != 1 {
            anyhow::bail!("subquery must return only one column");
        }
        match out.rows.len() {
            0 => Ok(Value::Null),
            1 => Ok(out
                .rows
                .into_iter()
                .next()
                .and_then(|row| row.values.into_iter().next())
                .unwrap_or(Value::Null)),
            _ => anyhow::bail!("more than one row returned by a subquery used as an expression"),
        }
    }

    pub(crate) fn exec_set_op(
        &self,
        ctx: &ExecutionContext,
        op: SetOpKind,
        all: bool,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    ) -> Result<QueryOutput> {
        let mut left_out = self.execute_logical_inner(ctx, *left)?;
        let right_out = self.execute_logical_inner(ctx, *right)?;
        // Column names/types come from the left input (SQL semantics).
        left_out.rows = set_op_rows(op, all, left_out.rows, right_out.rows);
        Ok(left_out)
    }
}

fn row_key(row: &Row) -> String {
    row.values
        .iter()
        .map(|v| render(&crate::value::key_form(v)))
        .collect::<Vec<_>>()
        .join("\u{1}")
}

fn dedup_rows(rows: Vec<Row>) -> Vec<Row> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for r in rows {
        if seen.insert(row_key(&r)) {
            out.push(r);
        }
    }
    out
}

/// Combines two row sets by SQL set-operation multiset semantics. `ALL` keeps
/// duplicates; otherwise the result is distinct. Column names/types are the
/// caller's responsibility (they come from the left input).
fn set_op_rows(op: SetOpKind, all: bool, left: Vec<Row>, right: Vec<Row>) -> Vec<Row> {
    match op {
        SetOpKind::Union => {
            let mut out = left;
            out.extend(right);
            if all { out } else { dedup_rows(out) }
        }
        SetOpKind::Intersect => {
            let mut right_counts: HashMap<String, usize> = HashMap::new();
            for r in &right {
                *right_counts.entry(row_key(r)).or_insert(0) += 1;
            }
            let mut emitted: HashMap<String, usize> = HashMap::new();
            let mut out = Vec::new();
            for r in left {
                let k = row_key(&r);
                let available = right_counts.get(&k).copied().unwrap_or(0);
                let used = emitted.entry(k).or_insert(0);
                let keep = if all {
                    *used < available
                } else {
                    *used == 0 && available > 0
                };
                *used += 1;
                if keep {
                    out.push(r);
                }
            }
            out
        }
        SetOpKind::Except => {
            let mut right_counts: HashMap<String, usize> = HashMap::new();
            for r in &right {
                *right_counts.entry(row_key(r)).or_insert(0) += 1;
            }
            let mut emitted: HashMap<String, usize> = HashMap::new();
            let mut out = Vec::new();
            for r in left {
                let k = row_key(&r);
                let right_n = right_counts.get(&k).copied().unwrap_or(0);
                let used = emitted.entry(k).or_insert(0);
                let keep = if all {
                    *used >= right_n
                } else {
                    *used == 0 && right_n == 0
                };
                *used += 1;
                if keep {
                    out.push(r);
                }
            }
            out
        }
    }
}
