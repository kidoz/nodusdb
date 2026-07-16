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
        AggregateOp::Sum => {
            let mut sum_int = 0i64;
            let mut sum_float = 0f64;
            let mut is_float = false;
            for r in group_rows {
                if let Some(v) = idx.and_then(|i| r.get(i)) {
                    match v {
                        Value::Int(n) => {
                            if is_float {
                                sum_float += (*n) as f64
                            } else {
                                sum_int += n
                            }
                        }
                        Value::Float(f) => {
                            if !is_float {
                                sum_float = sum_int as f64;
                                is_float = true;
                            }
                            sum_float += f;
                        }
                        _ => {}
                    }
                }
            }
            if group_rows.is_empty() {
                Value::Null
            } else if is_float {
                Value::Float(sum_float)
            } else {
                Value::Int(sum_int)
            }
        }
        AggregateOp::Min | AggregateOp::Max => {
            let want_less = matches!(op, AggregateOp::Min);
            let mut acc: Option<Value> = None;
            for r in group_rows {
                if let Some(v) = idx.and_then(|i| r.get(i)) {
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    let replace = match &acc {
                        Some(cur) => {
                            let ord = compare(v, cur);
                            if want_less {
                                ord == std::cmp::Ordering::Less
                            } else {
                                ord == std::cmp::Ordering::Greater
                            }
                        }
                        None => true,
                    };
                    if replace {
                        acc = Some(v.clone());
                    }
                }
            }
            acc.unwrap_or(Value::Null)
        }
        AggregateOp::Avg => {
            let mut sum = 0f64;
            let mut count = 0i64;
            for r in group_rows {
                if let Some(v) = idx.and_then(|i| r.get(i)) {
                    match v {
                        Value::Int(n) => {
                            sum += *n as f64;
                            count += 1;
                        }
                        Value::Float(f) => {
                            sum += *f;
                            count += 1;
                        }
                        _ => {}
                    }
                }
            }
            if count == 0 {
                Value::Null
            } else {
                Value::Float(sum / count as f64)
            }
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

/// Evaluates a scalar expression over an aggregated group: `Aggregate` nodes
/// compute over `group_rows`; a `Column` reads the group's representative
/// (first) row. Enables `sum(a) + 1`, `count(*) * 2`, etc.
pub(crate) fn eval_scalar_expr_grouped(
    expr: &ScalarExpr,
    group_rows: &[Vec<Value>],
    col_names: &[String],
) -> Value {
    use crate::planner::{
        apply_binary_op, apply_date_offset, apply_unary_op, cast_value, extract_datetime_field,
    };
    match expr {
        ScalarExpr::Aggregate { op, arg } => compute_aggregate(op, arg, group_rows, col_names),
        ScalarExpr::DateOffset {
            base,
            months,
            days,
            seconds,
        } => apply_date_offset(
            &eval_scalar_expr_grouped(base, group_rows, col_names),
            *months,
            *days,
            *seconds,
        ),
        ScalarExpr::Literal(v) => v.clone(),
        ScalarExpr::Column(name) => col_names
            .iter()
            .position(|c| c == name || c.ends_with(&format!(".{name}")))
            .and_then(|i| group_rows.first().and_then(|r| r.get(i)))
            .cloned()
            .unwrap_or(Value::Null),
        ScalarExpr::Unary { op, expr } => {
            apply_unary_op(*op, eval_scalar_expr_grouped(expr, group_rows, col_names))
        }
        ScalarExpr::Binary { op, left, right } => apply_binary_op(
            *op,
            eval_scalar_expr_grouped(left, group_rows, col_names),
            eval_scalar_expr_grouped(right, group_rows, col_names),
        ),
        ScalarExpr::Cast { expr, target } => {
            cast_value(eval_scalar_expr_grouped(expr, group_rows, col_names), target)
        }
        ScalarExpr::Function { name, args } => {
            let vals: Vec<Value> = args
                .iter()
                .map(|a| eval_scalar_expr_grouped(a, group_rows, col_names))
                .collect();
            crate::eval_scalar_function(name, &vals)
        }
        ScalarExpr::IsNull { expr, negated } => {
            let is_null =
                matches!(eval_scalar_expr_grouped(expr, group_rows, col_names), Value::Null);
            Value::Bool(if *negated { !is_null } else { is_null })
        }
        ScalarExpr::Extract { field, expr } => {
            extract_datetime_field(&eval_scalar_expr_grouped(expr, group_rows, col_names), field)
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
        // Other shapes (LIKE/IN/subquery) are not meaningful in HAVING here.
        _ => true,
    }
}
