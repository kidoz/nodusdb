//! Aggregate computation (COUNT/SUM/MIN/MAX) and HAVING predicate evaluation,
//! shared by the SELECT executor and the GROUP BY path.

use crate::*;

/// Computes one aggregate over a group's rows. `inner` is the aggregated column
/// name (or `*` for `COUNT(*)`). Shared by SELECT projection and HAVING.
pub(crate) fn compute_aggregate(
    op: &AggregateOp,
    inner: &str,
    group_rows: &[Vec<Value>],
    col_names: &[String],
) -> Value {
    let mut idx = col_names
        .iter()
        .position(|tc| tc == inner || tc.ends_with(&format!(".{inner}")));
    if inner == "*" {
        idx = Some(0);
    }
    match op {
        AggregateOp::Count => {
            let count = if inner == "*" {
                group_rows.len() as i64
            } else {
                group_rows
                    .iter()
                    .filter(|r| {
                        idx.and_then(|i| r.get(i))
                            .is_some_and(|v| !matches!(v, Value::Null))
                    })
                    .count() as i64
            };
            Value::Int(count)
        }
        // Every other aggregate skips NULLs, and is NULL over no values.
        _ => {
            let values: Vec<Value> = group_rows
                .iter()
                .map(|r| idx.and_then(|i| r.get(i)).cloned().unwrap_or(Value::Null))
                .collect();
            aggregate_values(op, &values)
        }
    }
}

/// Parses a HAVING predicate left-hand side: an aggregate key like `SUM(amount)`
/// or `COUNT(*)`, otherwise `None` (a plain group column).
pub(crate) fn parse_aggregate_key(key: &str) -> Option<(AggregateOp, String)> {
    let open = key.find('(')?;
    if !key.ends_with(')') {
        return None;
    }
    let func = key[..open].to_ascii_uppercase();
    let arg = key[open + 1..key.len() - 1].to_string();
    let op = match func.as_str() {
        "COUNT" => AggregateOp::Count,
        "SUM" => AggregateOp::Sum,
        "MIN" => AggregateOp::Min,
        "MAX" => AggregateOp::Max,
        _ => return None,
    };
    Some((op, arg))
}

/// Aggregates a list of already-computed per-row values (used for aggregates
/// over expressions, e.g. `sum(a + 1)`). NULLs are skipped, matching SQL
/// aggregate semantics; an all-NULL/empty input yields NULL (0 for COUNT).
pub(crate) fn aggregate_values(op: &AggregateOp, vals: &[Value]) -> Value {
    let non_null: Vec<&Value> = vals.iter().filter(|v| **v != Value::Null).collect();
    match op {
        AggregateOp::Count => Value::Int(non_null.len() as i64),
        AggregateOp::Sum | AggregateOp::Avg => {
            if non_null.is_empty() {
                return Value::Null;
            }
            let mut int_sum = 0i64;
            let mut float_sum = 0f64;
            let mut is_float = false;
            for v in &non_null {
                match v {
                    Value::Int(i) => {
                        int_sum = match int_sum.checked_add(*i) {
                            Some(sum) => sum,
                            None => return crate::eval_error::raise("bigint out of range"),
                        };
                        float_sum += *i as f64;
                    }
                    Value::Float(f) => {
                        is_float = true;
                        float_sum += f;
                    }
                    _ => return Value::Null,
                }
            }
            if *op == AggregateOp::Avg {
                Value::Float(float_sum / non_null.len() as f64)
            } else if is_float {
                Value::Float(float_sum)
            } else {
                Value::Int(int_sum)
            }
        }
        AggregateOp::Min | AggregateOp::Max => {
            let mut best: Option<&Value> = None;
            for v in non_null {
                best = Some(match best {
                    None => v,
                    Some(b) => {
                        let ord = crate::compare(v, b);
                        let take = if *op == AggregateOp::Min {
                            ord == std::cmp::Ordering::Less
                        } else {
                            ord == std::cmp::Ordering::Greater
                        };
                        if take { v } else { b }
                    }
                });
            }
            best.cloned().unwrap_or(Value::Null)
        }
    }
}

/// Evaluates a scalar expression over an aggregated group: `Aggregate` nodes
/// compute over `group_rows`; a `Column` reads the group's representative
/// (first) row. Enables `sum(a) + 1`, `count(*) * 2`, etc.
pub(crate) fn eval_scalar_expr_grouped(
    expr: &ScalarExpr,
    group_rows: &[Vec<Value>],
    col_names: &[String],
) -> Value {
    crate::planner::eval_scalar_in(
        expr,
        &GroupScope {
            group_rows,
            col_names,
        },
    )
}

struct GroupScope<'a> {
    group_rows: &'a [Vec<Value>],
    col_names: &'a [String],
}

impl crate::planner::ScalarScope for GroupScope<'_> {
    fn column(&self, name: &str) -> Value {
        crate::filter_eval::col_pos(self.col_names, name)
            .and_then(|i| self.group_rows.first().and_then(|r| r.get(i)))
            .cloned()
            .unwrap_or(Value::Null)
    }

    fn aggregate(&self, expr: &ScalarExpr) -> Value {
        let ScalarExpr::Aggregate {
            op,
            arg,
            arg_expr,
            distinct,
        } = expr
        else {
            return Value::Null;
        };
        let (group_rows, col_names) = (self.group_rows, self.col_names);
        if *distinct {
            // `agg(DISTINCT x)`: gather the per-row argument values,
            // drop duplicates, then aggregate the distinct set.
            let mut vals: Vec<Value> = Vec::new();
            for r in group_rows {
                let v = match arg_expr {
                    Some(e) => crate::planner::eval_scalar_expr(e, r, col_names),
                    None => crate::filter_eval::col_pos(col_names, arg)
                        .and_then(|i| r.get(i))
                        .cloned()
                        .unwrap_or(Value::Null),
                };
                if !vals.iter().any(|x| crate::values_equal(x, &v)) {
                    vals.push(v);
                }
            }
            return aggregate_values(op, &vals);
        }
        match arg_expr {
            // Aggregate over a computed expression: evaluate it per row,
            // then aggregate the resulting values.
            Some(e) => {
                let vals: Vec<Value> = group_rows
                    .iter()
                    .map(|r| crate::planner::eval_scalar_expr(e, r, col_names))
                    .collect();
                aggregate_values(op, &vals)
            }
            None => compute_aggregate(op, arg, group_rows, col_names),
        }
    }
}

/// Resolves a HAVING reference (aggregate key or group column) to a value.
pub(crate) fn having_value(
    name: &str,
    group_rows: &[Vec<Value>],
    col_names: &[String],
) -> Option<Value> {
    if let Some((op, arg)) = parse_aggregate_key(name) {
        return Some(compute_aggregate(&op, &arg, group_rows, col_names));
    }
    let idx = col_names
        .iter()
        .position(|tc| tc == name || tc.ends_with(&format!(".{name}")))?;
    group_rows.first().and_then(|r| r.get(idx)).cloned()
}

/// Coerces a numeric-looking text operand so comparisons are numeric, not lexical.
pub(crate) fn having_operand(
    op: &Operand,
    group_rows: &[Vec<Value>],
    col_names: &[String],
) -> Option<Value> {
    match op {
        Operand::Literal(Value::Text(s)) => {
            if let Ok(i) = s.parse::<i64>() {
                Some(Value::Int(i))
            } else if let Ok(f) = s.parse::<f64>() {
                Some(Value::Float(f))
            } else {
                Some(Value::Text(s.clone()))
            }
        }
        Operand::Literal(v) => Some(v.clone()),
        Operand::Ident(name) => having_value(name, group_rows, col_names),
    }
}

/// Evaluates a HAVING predicate against one aggregated group.
pub(crate) fn eval_having(
    expr: &FilterExpr,
    group_rows: &[Vec<Value>],
    col_names: &[String],
) -> bool {
    match expr {
        FilterExpr::And(l, r) => {
            eval_having(l, group_rows, col_names) && eval_having(r, group_rows, col_names)
        }
        FilterExpr::Or(l, r) => {
            eval_having(l, group_rows, col_names) || eval_having(r, group_rows, col_names)
        }
        FilterExpr::Not(inner) => !eval_having(inner, group_rows, col_names),
        FilterExpr::Predicate(p) => {
            let (Some(left), Some(right)) = (
                having_value(&p.left, group_rows, col_names),
                having_operand(&p.right, group_rows, col_names),
            ) else {
                return false;
            };
            let ord = compare(&left, &right);
            match p.op {
                CompareOp::Eq => left == right,
                CompareOp::Ne => left != right,
                CompareOp::Lt => ord == std::cmp::Ordering::Less,
                CompareOp::Le => ord != std::cmp::Ordering::Greater,
                CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                CompareOp::Ge => ord != std::cmp::Ordering::Less,
                _ => false,
            }
        }
        // A computed comparison, e.g. `HAVING min(a) = max(a)`: both sides
        // evaluate over the group (aggregates included).
        FilterExpr::ExprCmp { left, op, right } => {
            let l = eval_scalar_expr_grouped(left, group_rows, col_names);
            let r = eval_scalar_expr_grouped(right, group_rows, col_names);
            if l == Value::Null || r == Value::Null {
                return false;
            }
            let ord = compare(&l, &r);
            match op {
                CompareOp::Eq => crate::values_equal(&l, &r),
                CompareOp::Ne => !crate::values_equal(&l, &r),
                CompareOp::Lt => ord == std::cmp::Ordering::Less,
                CompareOp::Le => ord != std::cmp::Ordering::Greater,
                CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                CompareOp::Ge => ord != std::cmp::Ordering::Less,
                _ => false,
            }
        }
        // The planner lowers HAVING to one scalar condition over the group.
        FilterExpr::Scalar(e) => {
            eval_scalar_expr_grouped(e, group_rows, col_names) == Value::Bool(true)
        }
        // Shapes the planner never produces for HAVING must not admit groups.
        _ => false,
    }
}
