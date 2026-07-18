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
    ) -> Result<QueryOutput> {
        let mut columns = Vec::new();
        let mut types = Vec::new();
        let mut row_values = Vec::new();

        for (alias, value, type_hint) in values {
            columns.push(alias);
            // A CAST's declared type wins (so `NULL::int` is int4); otherwise
            // infer from the value's variant.
            let ty = match type_hint {
                Some(t) => match crate::value::column_type(&t) {
                    crate::value::ColumnType::Int => "INTEGER",
                    crate::value::ColumnType::Float => "DOUBLE",
                    crate::value::ColumnType::Bool => "BOOLEAN",
                    crate::value::ColumnType::Text => "VARCHAR",
                }
                .to_string(),
                None => match &value {
                    Value::Int(_) => "INTEGER".to_string(),
                    Value::Float(_) => "DOUBLE".to_string(),
                    Value::Bool(_) => "BOOLEAN".to_string(),
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
        .map(render)
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
