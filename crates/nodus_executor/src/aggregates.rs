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
    let inputs: Vec<(Value, Vec<Value>)> = vals.iter().map(|v| (v.clone(), Vec::new())).collect();
    aggregate_inputs(op, &inputs)
}

/// Aggregates per-row inputs, in order: each row's argument value and its
/// further arguments (a delimiter, an object value). Every aggregate but
/// `array_agg` and the JSON aggregates skips NULL values; over no values each
/// is NULL except `count`, which is 0.
pub(crate) fn aggregate_inputs(op: &AggregateOp, inputs: &[(Value, Vec<Value>)]) -> Value {
    let values = || inputs.iter().map(|(v, _)| v);
    let non_null = || values().filter(|v| **v != Value::Null);
    match op {
        AggregateOp::Count
        | AggregateOp::Sum
        | AggregateOp::Avg
        | AggregateOp::Min
        | AggregateOp::Max => {
            let vals: Vec<Value> = values().cloned().collect();
            aggregate_numeric(op, &vals)
        }
        AggregateOp::StringAgg => {
            let mut out: Option<String> = None;
            for (value, extra) in inputs {
                if *value == Value::Null {
                    continue;
                }
                let text = crate::render(value);
                out = Some(match out {
                    // Each value after the first is preceded by its own row's
                    // delimiter; a NULL delimiter adds nothing.
                    Some(mut acc) => {
                        if let Some(d) = extra.first().filter(|d| **d != Value::Null) {
                            acc.push_str(&crate::render(d));
                        }
                        acc.push_str(&text);
                        acc
                    }
                    None => text,
                });
            }
            out.map_or(Value::Null, Value::Text)
        }
        AggregateOp::ArrayAgg => {
            if inputs.is_empty() {
                Value::Null
            } else {
                Value::Array(values().cloned().collect())
            }
        }
        AggregateOp::BoolAnd | AggregateOp::BoolOr => {
            let mut result: Option<bool> = None;
            for value in non_null() {
                let Value::Bool(b) = value else {
                    return crate::eval_error::raise(format!(
                        "function {}({}) does not exist",
                        op.sql_name(),
                        crate::value::value_type_name(value)
                    ));
                };
                result = Some(match (op, result) {
                    (AggregateOp::BoolAnd, Some(acc)) => acc && *b,
                    (_, Some(acc)) => acc || *b,
                    (_, None) => *b,
                });
            }
            result.map_or(Value::Null, Value::Bool)
        }
        AggregateOp::JsonAgg => {
            if inputs.is_empty() {
                return Value::Null;
            }
            // Arrays and rows each start a line after the first.
            let structured = values().any(|v| matches!(v, Value::Array(_) | Value::Record(_)));
            let mut out = String::from("[");
            for (i, value) in values().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                    if structured && *value != Value::Null {
                        out.push_str("\n ");
                    }
                }
                crate::json_text::value_json(value, false, &mut out);
            }
            out.push(']');
            Value::Json(out)
        }
        AggregateOp::JsonbAgg => {
            if inputs.is_empty() {
                Value::Null
            } else {
                Value::Jsonb(serde_json::Value::Array(
                    values().map(crate::functions::to_json).collect(),
                ))
            }
        }
        AggregateOp::JsonObjectAgg => {
            if inputs.is_empty() {
                return Value::Null;
            }
            let mut out = String::from("{ ");
            for (i, (key, extra)) in inputs.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                if let Err(e) = crate::json_text::key_json(key, &mut out) {
                    return crate::eval_error::raise(e);
                }
                out.push_str(" : ");
                crate::json_text::value_json(
                    extra.first().unwrap_or(&Value::Null),
                    false,
                    &mut out,
                );
            }
            out.push_str(" }");
            Value::Json(out)
        }
        AggregateOp::JsonbObjectAgg => {
            if inputs.is_empty() {
                return Value::Null;
            }
            let mut object = serde_json::Map::new();
            for (key, extra) in inputs {
                if *key == Value::Null {
                    return crate::eval_error::raise("null value not allowed for object key");
                }
                let value = extra
                    .first()
                    .map_or(serde_json::Value::Null, crate::functions::to_json);
                object.insert(crate::render(key), value);
            }
            Value::Jsonb(serde_json::Value::Object(object))
        }
        AggregateOp::StddevSamp
        | AggregateOp::StddevPop
        | AggregateOp::VarSamp
        | AggregateOp::VarPop => {
            let mut nums = Vec::new();
            for value in non_null() {
                match value {
                    Value::Int(i) => nums.push(*i as f64),
                    Value::Float(f) => nums.push(*f),
                    other => {
                        return crate::eval_error::raise(format!(
                            "function {}({}) does not exist",
                            op.sql_name(),
                            crate::value::value_type_name(other)
                        ));
                    }
                }
            }
            let sample = matches!(op, AggregateOp::StddevSamp | AggregateOp::VarSamp);
            let n = nums.len() as f64;
            if nums.is_empty() || (sample && nums.len() < 2) {
                return Value::Null;
            }
            let mean = nums.iter().sum::<f64>() / n;
            let squares = nums.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>();
            let variance = squares / if sample { n - 1.0 } else { n };
            Value::Float(match op {
                AggregateOp::StddevSamp | AggregateOp::StddevPop => variance.sqrt(),
                _ => variance,
            })
        }
        AggregateOp::BitAnd | AggregateOp::BitOr => {
            let mut result: Option<i64> = None;
            for value in non_null() {
                let Value::Int(i) = value else {
                    return crate::eval_error::raise(format!(
                        "function {}({}) does not exist",
                        op.sql_name(),
                        crate::value::value_type_name(value)
                    ));
                };
                result = Some(match (op, result) {
                    (AggregateOp::BitAnd, Some(acc)) => acc & i,
                    (_, Some(acc)) => acc | i,
                    (_, None) => *i,
                });
            }
            result.map_or(Value::Null, Value::Int)
        }
    }
}

/// `count`, `sum`, `avg`, `min`, and `max` over values.
fn aggregate_numeric(op: &AggregateOp, vals: &[Value]) -> Value {
    let non_null: Vec<&Value> = vals.iter().filter(|v| **v != Value::Null).collect();
    match op {
        AggregateOp::Count => Value::Int(non_null.len() as i64),
        AggregateOp::Sum | AggregateOp::Avg => {
            if non_null.is_empty() {
                return Value::Null;
            }
            // Integers and numerics sum exactly; `avg` of them is a numeric
            // with PostgreSQL's division scale. A float makes both floats.
            if !non_null.iter().any(|v| matches!(v, Value::Float(_))) {
                let mut sum = rust_decimal::Decimal::ZERO;
                let mut any_numeric = false;
                for v in &non_null {
                    let d = match v {
                        Value::Int(i) => rust_decimal::Decimal::from(*i),
                        Value::Numeric(d) => {
                            any_numeric = true;
                            *d
                        }
                        _ => return Value::Null,
                    };
                    sum = match sum.checked_add(d) {
                        Some(s) => s,
                        None => return crate::eval_error::raise("value overflows numeric format"),
                    };
                }
                return if *op == AggregateOp::Avg {
                    crate::planner::numeric_div(sum, rust_decimal::Decimal::from(non_null.len()))
                } else if any_numeric {
                    Value::Numeric(sum)
                } else {
                    use rust_decimal::prelude::ToPrimitive;
                    // An integer sum too large for bigint is a numeric.
                    sum.to_i64().map_or(Value::Numeric(sum), Value::Int)
                };
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
                    Value::Numeric(d) => {
                        is_float = true;
                        float_sum += crate::value::decimal_to_f64(d);
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
        // Routed by `aggregate_inputs`.
        _ => Value::Null,
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
            extra_args,
            filter,
            order_by,
        } = expr
        else {
            return Value::Null;
        };
        let (group_rows, col_names) = (self.group_rows, self.col_names);
        let eval = |e: &ScalarExpr, r: &[Value]| crate::planner::eval_scalar_expr(e, r, col_names);
        // Only rows where FILTER is true are aggregated.
        let rows: Vec<&Vec<Value>> = group_rows
            .iter()
            .filter(|r| {
                filter
                    .as_ref()
                    .is_none_or(|f| eval(f, r) == Value::Bool(true))
            })
            .collect();
        // `count(*)` counts rows rather than values.
        if arg == "*" && arg_expr.is_none() {
            return Value::Int(rows.len() as i64);
        }
        let column = crate::filter_eval::col_pos(col_names, arg);
        let mut inputs: Vec<(Vec<Value>, (Value, Vec<Value>))> = rows
            .iter()
            .map(|r| {
                let value = match arg_expr {
                    Some(e) => eval(e, r),
                    None => column
                        .and_then(|i| r.get(i))
                        .cloned()
                        .unwrap_or(Value::Null),
                };
                let extra = extra_args.iter().map(|e| eval(e, r)).collect();
                let keys = order_by.iter().map(|(e, _, _)| eval(e, r)).collect();
                (keys, (value, extra))
            })
            .collect();
        if !order_by.is_empty() {
            inputs.sort_by(|(a, _), (b, _)| {
                for (i, (_, ascending, nulls_first)) in order_by.iter().enumerate() {
                    let ord = crate::select::order_cmp(&a[i], &b[i], *ascending, *nulls_first);
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        let mut inputs: Vec<(Value, Vec<Value>)> = inputs.into_iter().map(|(_, i)| i).collect();
        if *distinct {
            // `agg(DISTINCT x)`: the first of each distinct value, in order.
            let mut kept: Vec<(Value, Vec<Value>)> = Vec::with_capacity(inputs.len());
            for input in inputs {
                if !kept.iter().any(|(v, _)| crate::values_equal(v, &input.0)) {
                    kept.push(input);
                }
            }
            inputs = kept;
        }
        aggregate_inputs(op, &inputs)
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
