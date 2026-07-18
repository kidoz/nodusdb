//! Expression-level parsing: SQL value/operand extraction and column-name resolution.
use super::*;
use crate::*;
use anyhow::Result;
use nodus_catalog::TableConstraint;

pub fn expr_to_value(expr: &sqlparser::ast::Expr, params: &[crate::Value]) -> Option<crate::Value> {
    use sqlparser::ast::{Expr, Value as SqlValue};
    match expr {
        Expr::Value(v) => match &v.value {
            SqlValue::SingleQuotedString(s) => Some(crate::Value::Text(s.clone())),
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Some(crate::Value::Int(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Some(crate::Value::Float(f))
                } else {
                    Some(crate::Value::Text(n.clone()))
                }
            }
            SqlValue::Boolean(b) => Some(crate::Value::Bool(*b)),
            SqlValue::Null => Some(crate::Value::Null),
            SqlValue::Placeholder(s) => {
                if let Some(stripped) = s.strip_prefix('$') {
                    if let Ok(idx) = stripped.parse::<usize>() {
                        if idx > 0 && idx <= params.len() {
                            return Some(params[idx - 1].clone());
                        }
                    }
                }
                None
            }
            _ => None,
        },
        Expr::Identifier(id) => Some(crate::Value::Text(id.value.clone())),
        // Typed string literals like `DATE '2024-06-15'` / `TIMESTAMP '...'`.
        // NodusDB has no native temporal type, so the value is kept as text
        // (ISO-8601 text compares/sorts chronologically).
        Expr::TypedString(ts) => match &ts.value.value {
            SqlValue::SingleQuotedString(s) => Some(crate::Value::Text(s.clone())),
            _ => None,
        },
        // `INTERVAL '1 day'` — NodusDB has no native interval type, so it's kept
        // as canonical PostgreSQL text (round-trips through INTERVAL columns).
        Expr::Interval(iv) => {
            parse_interval(iv).map(|(m, d, s)| crate::Value::Text(format_interval(m, d, s)))
        }
        Expr::Array(sqlparser::ast::Array { elem, .. }) => {
            let mut arr = Vec::new();
            for e in elem {
                if let Some(v) = expr_to_value(e, params) {
                    arr.push(v);
                } else {
                    return None;
                }
            }
            Some(crate::Value::Array(arr))
        }
        // Signed numeric literals: `-5`, `+3.2`.
        Expr::UnaryOp { op, expr: inner } => {
            let v = expr_to_value(inner, params)?;
            match op {
                sqlparser::ast::UnaryOperator::Minus => match v {
                    crate::Value::Int(i) => Some(crate::Value::Int(-i)),
                    crate::Value::Float(f) => Some(crate::Value::Float(-f)),
                    _ => None,
                },
                sqlparser::ast::UnaryOperator::Plus => Some(v),
                _ => None,
            }
        }
        Expr::Nested(inner) => expr_to_value(inner, params),
        _ => None,
    }
}

pub(crate) fn extract_col_name(expr: &sqlparser::ast::Expr) -> Option<String> {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(id) => Some(id.value.clone()),
        Expr::CompoundIdentifier(ids) => Some(
            ids.iter()
                .map(|id| id.value.clone())
                .collect::<Vec<_>>()
                .join("."),
        ),
        // PostgreSQL JSON access (`->`/`->>`/`#>`/`#>>`) parses as a binary op.
        Expr::BinaryOp { left, op, right }
            if matches!(
                op,
                sqlparser::ast::BinaryOperator::Arrow
                    | sqlparser::ast::BinaryOperator::LongArrow
                    | sqlparser::ast::BinaryOperator::HashArrow
                    | sqlparser::ast::BinaryOperator::HashLongArrow
            ) =>
        {
            let left_col = extract_col_name(left)?;
            let right_val = match &**right {
                Expr::Value(v) => match &v.value {
                    sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
                    sqlparser::ast::Value::Number(n, _) => n.clone(),
                    _ => return None,
                },
                _ => return None,
            };
            let op_str = match op {
                sqlparser::ast::BinaryOperator::LongArrow => "->>",
                sqlparser::ast::BinaryOperator::Arrow => "->",
                sqlparser::ast::BinaryOperator::HashArrow => "#>",
                sqlparser::ast::BinaryOperator::HashLongArrow => "#>>",
                _ => return None,
            };
            Some(format!("{}{}'{}'", left_col, op_str, right_val))
        }
        Expr::Cast { expr, .. } => extract_col_name(expr),
        // Aggregate function calls render to a canonical `FUNC(arg)` key so a
        // `HAVING` predicate can name them. Non-aggregate functions stay `None`
        // so they don't silently match in a `WHERE` clause.
        Expr::Function(func) => {
            use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
            let fname = func.name.to_string().to_uppercase();
            if !matches!(fname.as_str(), "COUNT" | "SUM" | "MIN" | "MAX" | "AVG") {
                return None;
            }
            let first_arg = match &func.args {
                FunctionArguments::List(list) => list.args.first(),
                _ => None,
            };
            let arg = match first_arg {
                Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) => "*".to_string(),
                Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => extract_col_name(e)?,
                _ => return None,
            };
            Some(format!("{fname}({arg})"))
        }
        _ => None,
    }
}

pub(crate) fn parse_simple_case_when_eq(
    expr: &sqlparser::ast::Expr,
    alias: Option<String>,
    params: &[Value],
) -> Option<ProjectionItem> {
    use sqlparser::ast::{BinaryOperator, Expr};
    let Expr::Case {
        operand: None,
        conditions,
        else_result: Some(else_result),
        ..
    } = expr
    else {
        return None;
    };
    let when = conditions.first()?;
    let condition = &when.condition;
    let then_expr = &when.result;
    let Expr::BinaryOp { left, op, right } = condition else {
        return None;
    };
    if *op != BinaryOperator::Eq {
        return None;
    }
    let left = extract_col_name(left)?;
    let equals = expr_to_value(right, params)?;
    let (then_value, then_column) = if let Some(value) = expr_to_value(then_expr, params) {
        (value, None)
    } else {
        (Value::Null, Some(extract_col_name(then_expr)?))
    };
    let else_column = extract_col_name(else_result)?;
    Some(ProjectionItem::CaseWhenEq {
        left,
        equals,
        then_value,
        then_column,
        else_column,
        alias,
    })
}

/// Parses a searched or simple `CASE` into a general multi-branch projection.
/// Simple `CASE x WHEN v` becomes the predicate `x = v`; searched `CASE WHEN
/// <pred>` parses the predicate directly. Branches whose predicate can't be
/// parsed are skipped.
pub(crate) fn parse_case(
    expr: &sqlparser::ast::Expr,
    alias: Option<String>,
    params: &[Value],
) -> Option<ProjectionItem> {
    use sqlparser::ast::Expr;
    let Expr::Case {
        operand,
        conditions,
        else_result,
        ..
    } = expr
    else {
        return None;
    };
    let mut branches = Vec::new();
    for when in conditions.iter() {
        let cond = &when.condition;
        let res = &when.result;
        let pred = match operand {
            // Simple CASE: `operand = cond`.
            Some(op_expr) => Predicate {
                left: extract_col_name(op_expr)?,
                op: CompareOp::Eq,
                right: extract_operand(cond, params)?,
            },
            // Searched CASE: `cond` is a single-comparison predicate.
            None => match parse_filter_expr(cond, params)? {
                FilterExpr::Predicate(p) => p,
                _ => return None,
            },
        };
        let then = extract_operand(res, params)?;
        branches.push((pred, then));
    }
    if branches.is_empty() {
        return None;
    }
    let else_result = match else_result {
        Some(e) => extract_operand(e, params),
        None => None,
    };
    Some(ProjectionItem::Case {
        branches,
        else_result,
        alias,
    })
}

pub(crate) fn extract_operand(expr: &sqlparser::ast::Expr, params: &[Value]) -> Option<Operand> {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(id) => Some(Operand::Ident(id.value.clone())),
        Expr::CompoundIdentifier(ids) => Some(Operand::Ident(
            ids.iter()
                .map(|id| id.value.clone())
                .collect::<Vec<_>>()
                .join("."),
        )),
        _ => {
            if let Some(val) = expr_to_value(expr, params) {
                Some(Operand::Literal(val))
            } else {
                None
            }
        }
    }
}

/// Constant-folds a column-free scalar expression to a [`Value`] (for
/// FROM-less `SELECT <expr>`). Returns `None` for forms we don't evaluate, so
/// the caller can fall back to legacy handling. Handles literals, arithmetic,
/// comparisons, logical AND/OR, string `||`, unary `-`/`NOT`, `CAST`, `IS
/// [NOT] NULL`, and the scalar functions `eval_scalar_function` implements.
pub(crate) fn fold_scalar(expr: &sqlparser::ast::Expr, params: &[Value]) -> Option<Value> {
    use sqlparser::ast::{BinaryOperator as B, Expr, UnaryOperator as U};
    match expr {
        // True literals only; a bare identifier is not a constant.
        Expr::Value(_) | Expr::Array(_) | Expr::TypedString(_) => expr_to_value(expr, params),
        Expr::Nested(inner) => fold_scalar(inner, params),
        Expr::UnaryOp { op, expr: inner } => {
            let v = fold_scalar(inner, params)?;
            match op {
                U::Minus => match v {
                    Value::Int(i) => Some(Value::Int(-i)),
                    Value::Float(f) => Some(Value::Float(-f)),
                    Value::Null => Some(Value::Null),
                    _ => None,
                },
                U::Plus => Some(v),
                U::Not => match v {
                    Value::Bool(b) => Some(Value::Bool(!b)),
                    Value::Null => Some(Value::Null),
                    _ => None,
                },
                _ => None,
            }
        }
        Expr::BinaryOp { left, op, right } => {
            // JSON arrow operators are handled by the JsonAccess projection path.
            if matches!(
                op,
                B::Arrow | B::LongArrow | B::HashArrow | B::HashLongArrow
            ) {
                return None;
            }
            // Any binary op involving an INTERVAL (arithmetic or comparison) is
            // evaluated via the interval-aware lowering path, not `fold_binary`.
            if matches!(&**left, Expr::Interval(_)) || matches!(&**right, Expr::Interval(_)) {
                return lower_scalar(expr, params).map(|se| eval_scalar_expr(&se, &[], &[]));
            }
            let l = fold_scalar(left, params)?;
            let r = fold_scalar(right, params)?;
            fold_binary(op, l, r)
        }
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let v = fold_scalar(inner, params)?;
            Some(cast_value(v, &data_type.to_string()))
        }
        Expr::IsNull(inner) => Some(Value::Bool(matches!(
            fold_scalar(inner, params)?,
            Value::Null
        ))),
        Expr::IsNotNull(inner) => Some(Value::Bool(!matches!(
            fold_scalar(inner, params)?,
            Value::Null
        ))),
        Expr::Function(func) => fold_function(func, params),
        // SUBSTRING/TRIM/CASE parse to dedicated nodes; reuse the lowering +
        // evaluator (no columns are referenced in a FROM-less SELECT).
        Expr::Substring { .. } | Expr::Trim { .. } | Expr::Extract { .. } | Expr::Case { .. } => {
            lower_scalar(expr, params).map(|se| eval_scalar_expr(&se, &[], &[]))
        }
        _ => None,
    }
}

fn fold_binary(op: &sqlparser::ast::BinaryOperator, l: Value, r: Value) -> Option<Value> {
    use sqlparser::ast::BinaryOperator as B;
    let as_f64 = |v: &Value| -> Option<f64> {
        match v {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        }
    };
    match op {
        B::Plus | B::Minus | B::Multiply | B::Divide | B::Modulo => {
            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                return Some(Value::Null);
            }
            if let (Value::Int(a), Value::Int(b)) = (&l, &r) {
                let out = match op {
                    B::Plus => a.checked_add(*b),
                    B::Minus => a.checked_sub(*b),
                    B::Multiply => a.checked_mul(*b),
                    // PostgreSQL integer division truncates toward zero (Rust `/`).
                    B::Divide if *b != 0 => Some(a / b),
                    B::Modulo if *b != 0 => Some(a % b),
                    // Division/modulo by zero: PostgreSQL errors; surface NULL
                    // rather than panicking or returning a wrong number.
                    B::Divide | B::Modulo => return Some(Value::Null),
                    _ => return None,
                };
                // Overflow -> NULL (avoid a panic in a query path).
                Some(out.map(Value::Int).unwrap_or(Value::Null))
            } else {
                let (a, b) = (as_f64(&l)?, as_f64(&r)?);
                let out = match op {
                    B::Plus => a + b,
                    B::Minus => a - b,
                    B::Multiply => a * b,
                    B::Divide if b != 0.0 => a / b,
                    B::Modulo if b != 0.0 => a % b,
                    B::Divide | B::Modulo => return Some(Value::Null),
                    _ => return None,
                };
                Some(Value::Float(out))
            }
        }
        B::Eq | B::NotEq | B::Lt | B::LtEq | B::Gt | B::GtEq => {
            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                return Some(Value::Null);
            }
            use std::cmp::Ordering::{Greater, Less};
            let ord = compare(&l, &r);
            let b = match op {
                B::Eq => values_equal(&l, &r),
                B::NotEq => !values_equal(&l, &r),
                B::Lt => ord == Less,
                B::LtEq => ord != Greater,
                B::Gt => ord == Greater,
                B::GtEq => ord != Less,
                _ => return None,
            };
            Some(Value::Bool(b))
        }
        B::StringConcat => {
            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                Some(Value::Null)
            } else {
                Some(Value::Text(format!("{}{}", render(&l), render(&r))))
            }
        }
        B::And | B::Or => {
            // Three-valued logic; operands must be Bool or Null.
            let lb = match l {
                Value::Bool(b) => Some(b),
                Value::Null => None,
                _ => return None,
            };
            let rb = match r {
                Value::Bool(b) => Some(b),
                Value::Null => None,
                _ => return None,
            };
            let out = match op {
                B::And => match (lb, rb) {
                    (Some(false), _) | (_, Some(false)) => Value::Bool(false),
                    (Some(true), Some(true)) => Value::Bool(true),
                    _ => Value::Null,
                },
                B::Or => match (lb, rb) {
                    (Some(true), _) | (_, Some(true)) => Value::Bool(true),
                    (Some(false), Some(false)) => Value::Bool(false),
                    _ => Value::Null,
                },
                _ => return None,
            };
            Some(out)
        }
        _ => None,
    }
}

/// Casts a value to the logical category of `data_type` (INT/FLOAT/BOOL/TEXT).
/// NodusDB has no distinct NUMERIC type, so NUMERIC/DECIMAL fold to float.
pub(crate) fn cast_value(v: Value, data_type: &str) -> Value {
    use crate::value::ColumnType;
    if matches!(v, Value::Null) {
        return Value::Null;
    }
    match crate::value::column_type(data_type) {
        // PostgreSQL rounds half-to-even when casting to an integer.
        ColumnType::Int => match &v {
            Value::Int(_) => v,
            Value::Float(f) => Value::Int(f.round_ties_even() as i64),
            Value::Bool(b) => Value::Int(i64::from(*b)),
            Value::Text(s) => s
                .trim()
                .parse::<i64>()
                .map(Value::Int)
                .or_else(|_| {
                    s.trim()
                        .parse::<f64>()
                        .map(|f| Value::Int(f.round_ties_even() as i64))
                })
                .unwrap_or(Value::Null),
            _ => Value::Null,
        },
        ColumnType::Float => match &v {
            Value::Float(_) => v,
            Value::Int(i) => Value::Float(*i as f64),
            Value::Bool(b) => Value::Float(if *b { 1.0 } else { 0.0 }),
            Value::Text(s) => s.trim().parse::<f64>().map(Value::Float).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        ColumnType::Bool => match &v {
            Value::Bool(_) => v,
            Value::Int(i) => Value::Bool(*i != 0),
            Value::Text(s) => parse_bool_text(s),
            _ => Value::Null,
        },
        // Booleans cast to the SQL spellings, not the wire `t`/`f` rendering.
        ColumnType::Text => match &v {
            Value::Bool(b) => Value::Text(if *b { "true" } else { "false" }.to_string()),
            _ => Value::Text(render(&v)),
        },
    }
}

/// Extracts a datetime field (`YEAR`/`MONTH`/`DAY`/`HOUR`/`MINUTE`/`SECOND`)
/// from an ISO-8601 date/timestamp text value. Returns NULL when it can't parse.
pub(crate) fn extract_datetime_field(v: &Value, field: &str) -> Value {
    let text = match v {
        Value::Null => return Value::Null,
        Value::Text(s) => s.clone(),
        other => render(other),
    };
    let (date_part, time_part) = match text.trim().split_once([' ', 'T']) {
        Some((d, t)) => (d, Some(t)),
        None => (text.trim(), None),
    };
    let date_bits: Vec<&str> = date_part.split('-').collect();
    let field_up = field.to_ascii_uppercase();
    let parsed = match field_up.as_str() {
        "YEAR" => date_bits.first().and_then(|s| s.parse::<i64>().ok()),
        "MONTH" => date_bits.get(1).and_then(|s| s.parse::<i64>().ok()),
        "DAY" => date_bits.get(2).and_then(|s| s.parse::<i64>().ok()),
        "HOUR" | "MINUTE" | "SECOND" => {
            let time_bits: Vec<&str> = time_part.unwrap_or("").split(':').collect();
            let idx = match field_up.as_str() {
                "HOUR" => 0,
                "MINUTE" => 1,
                _ => 2,
            };
            // A SECOND field may carry a fraction (`08.5`); take the whole part.
            time_bits
                .get(idx)
                .and_then(|s| s.trim().split('.').next())
                .and_then(|s| s.trim().parse::<i64>().ok())
        }
        _ => None,
    };
    parsed.map(Value::Int).unwrap_or(Value::Null)
}

/// PostgreSQL-style textual boolean input; unrecognized text folds to NULL.
fn parse_bool_text(s: &str) -> Value {
    match s.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "y" | "yes" | "on" | "1" => Value::Bool(true),
        "f" | "false" | "n" | "no" | "off" | "0" => Value::Bool(false),
        _ => Value::Null,
    }
}

/// Folds a scalar function call whose arguments are all constant-foldable and
/// whose name `eval_scalar_function` implements; otherwise `None` (so niladic
/// specials like `version()` fall through to legacy handling).
fn fold_function(func: &sqlparser::ast::Function, params: &[Value]) -> Option<Value> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    let name = func.name.to_string().to_uppercase();
    if !is_foldable_scalar_fn(&name) {
        return None;
    }
    let FunctionArguments::List(list) = &func.args else {
        return None;
    };
    let mut args = Vec::with_capacity(list.args.len());
    for a in &list.args {
        match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => args.push(fold_scalar(e, params)?),
            _ => return None,
        }
    }
    Some(eval_scalar_function(&name, &args))
}

/// The scalar functions `eval_scalar_function` evaluates. Kept in sync with it
/// so unknown names fall through to the legacy niladic-function path.
fn is_foldable_scalar_fn(name: &str) -> bool {
    matches!(
        name,
        "CONCAT"
            | "UPPER"
            | "LOWER"
            | "LENGTH"
            | "CHAR_LENGTH"
            | "CHARACTER_LENGTH"
            | "TRIM"
            | "LTRIM"
            | "RTRIM"
            | "COALESCE"
            | "NULLIF"
            | "ABS"
            | "ROUND"
            | "REPLACE"
            | "SUBSTR"
            | "SUBSTRING"
            | "DATE_TRUNC"
            | "AGE"
    )
}

/// Lowers a SQL scalar expression into a serializable [`ScalarExpr`] for
/// per-row evaluation in a table projection. Returns `None` for forms not yet
/// supported, so the planner can fall back to its existing handling.
pub(crate) fn lower_scalar(
    expr: &sqlparser::ast::Expr,
    params: &[Value],
) -> Option<ScalarExpr> {
    use sqlparser::ast::{BinaryOperator as B, Expr, UnaryOperator as U};
    match expr {
        Expr::Value(_) | Expr::Array(_) | Expr::TypedString(_) | Expr::Interval(_) => {
            expr_to_value(expr, params).map(ScalarExpr::Literal)
        }
        Expr::Identifier(id) => Some(ScalarExpr::Column(id.value.clone())),
        Expr::CompoundIdentifier(ids) => Some(ScalarExpr::Column(
            ids.iter().map(|id| id.value.clone()).collect::<Vec<_>>().join("."),
        )),
        Expr::Nested(inner) => lower_scalar(inner, params),
        Expr::UnaryOp { op, expr: inner } => {
            let e = lower_scalar(inner, params)?;
            let op = match op {
                U::Minus => ScalarUnaryOp::Neg,
                U::Not => ScalarUnaryOp::Not,
                U::Plus => return Some(e),
                _ => return None,
            };
            Some(ScalarExpr::Unary {
                op,
                expr: Box::new(e),
            })
        }
        Expr::BinaryOp { left, op, right } => {
            // `date/timestamp ± INTERVAL` -> a resolved offset. Only when exactly
            // one side is an interval; interval ± interval falls through to the
            // general Binary path (evaluated by `apply_binary_op`).
            if matches!(op, B::Plus | B::Minus) {
                if let Expr::Interval(iv) = &**right
                    && !matches!(&**left, Expr::Interval(_))
                {
                    let (m, d, s) = parse_interval(iv)?;
                    let sign = if matches!(op, B::Minus) { -1 } else { 1 };
                    return Some(ScalarExpr::DateOffset {
                        base: Box::new(lower_scalar(left, params)?),
                        months: m * sign,
                        days: d * sign,
                        seconds: s * sign,
                    });
                }
                if matches!(op, B::Plus)
                    && let Expr::Interval(iv) = &**left
                    && !matches!(&**right, Expr::Interval(_))
                {
                    let (m, d, s) = parse_interval(iv)?;
                    return Some(ScalarExpr::DateOffset {
                        base: Box::new(lower_scalar(right, params)?),
                        months: m,
                        days: d,
                        seconds: s,
                    });
                }
            }
            let op = match op {
                B::Plus => ScalarBinaryOp::Add,
                B::Minus => ScalarBinaryOp::Sub,
                B::Multiply => ScalarBinaryOp::Mul,
                B::Divide => ScalarBinaryOp::Div,
                B::Modulo => ScalarBinaryOp::Mod,
                B::Eq => ScalarBinaryOp::Eq,
                B::NotEq => ScalarBinaryOp::NotEq,
                B::Lt => ScalarBinaryOp::Lt,
                B::LtEq => ScalarBinaryOp::LtEq,
                B::Gt => ScalarBinaryOp::Gt,
                B::GtEq => ScalarBinaryOp::GtEq,
                B::And => ScalarBinaryOp::And,
                B::Or => ScalarBinaryOp::Or,
                B::StringConcat => ScalarBinaryOp::Concat,
                // JSON arrows and everything else stay on their existing paths.
                _ => return None,
            };
            Some(ScalarExpr::Binary {
                op,
                left: Box::new(lower_scalar(left, params)?),
                right: Box::new(lower_scalar(right, params)?),
            })
        }
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => Some(ScalarExpr::Cast {
            expr: Box::new(lower_scalar(inner, params)?),
            target: data_type.to_string(),
        }),
        Expr::IsNull(inner) => Some(ScalarExpr::IsNull {
            expr: Box::new(lower_scalar(inner, params)?),
            negated: false,
        }),
        Expr::IsNotNull(inner) => Some(ScalarExpr::IsNull {
            expr: Box::new(lower_scalar(inner, params)?),
            negated: true,
        }),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let operand = match operand {
                Some(o) => Some(Box::new(lower_scalar(o, params)?)),
                None => None,
            };
            let mut branches = Vec::with_capacity(conditions.len());
            for when in conditions {
                branches.push((
                    lower_scalar(&when.condition, params)?,
                    lower_scalar(&when.result, params)?,
                ));
            }
            let else_result = match else_result {
                Some(e) => Some(Box::new(lower_scalar(e, params)?)),
                None => None,
            };
            Some(ScalarExpr::Case {
                operand,
                branches,
                else_result,
            })
        }
        Expr::Function(func) => {
            use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
            let name = func.name.to_string().to_uppercase();
            // An aggregate nested in an expression, e.g. `sum(a) + 1`.
            if let Some(op) = aggregate_op(&name) {
                let FunctionArguments::List(list) = &func.args else {
                    return None;
                };
                return match list.args.first() {
                    Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) => {
                        Some(ScalarExpr::Aggregate {
                            op,
                            arg: "*".to_string(),
                            arg_expr: None,
                        })
                    }
                    Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => {
                        match extract_col_name(e) {
                            Some(col) => Some(ScalarExpr::Aggregate {
                                op,
                                arg: col,
                                arg_expr: None,
                            }),
                            // Aggregate over a computed expression, e.g. `sum(a + 1)`.
                            None => Some(ScalarExpr::Aggregate {
                                op,
                                arg: String::new(),
                                arg_expr: Some(Box::new(lower_scalar(e, params)?)),
                            }),
                        }
                    }
                    _ => None,
                };
            }
            if !is_foldable_scalar_fn(&name) {
                return None;
            }
            let FunctionArguments::List(list) = &func.args else {
                return None;
            };
            let mut args = Vec::with_capacity(list.args.len());
            for a in &list.args {
                match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                        args.push(lower_scalar(e, params)?)
                    }
                    _ => return None,
                }
            }
            Some(ScalarExpr::Function { name, args })
        }
        // sqlparser lowers SUBSTRING/SUBSTR and TRIM to dedicated AST nodes
        // rather than `Expr::Function`; map them onto the scalar functions
        // `eval_scalar_function` already implements.
        Expr::Substring {
            expr: inner,
            substring_from,
            substring_for,
            ..
        } => {
            let mut args = vec![lower_scalar(inner, params)?];
            match (substring_from, substring_for) {
                (Some(from), Some(len)) => {
                    args.push(lower_scalar(from, params)?);
                    args.push(lower_scalar(len, params)?);
                }
                (Some(from), None) => args.push(lower_scalar(from, params)?),
                // `SUBSTRING(x FOR n)` starts at position 1.
                (None, Some(len)) => {
                    args.push(ScalarExpr::Literal(Value::Int(1)));
                    args.push(lower_scalar(len, params)?);
                }
                (None, None) => {}
            }
            Some(ScalarExpr::Function {
                name: "SUBSTR".to_string(),
                args,
            })
        }
        Expr::Trim {
            expr: inner,
            trim_where,
            ..
        } => {
            use sqlparser::ast::TrimWhereField;
            let name = match trim_where {
                Some(TrimWhereField::Leading) => "LTRIM",
                Some(TrimWhereField::Trailing) => "RTRIM",
                // BOTH or unspecified. `trim_what`/`trim_characters` are ignored:
                // `eval_scalar_function` trims whitespace only.
                _ => "TRIM",
            };
            Some(ScalarExpr::Function {
                name: name.to_string(),
                args: vec![lower_scalar(inner, params)?],
            })
        }
        Expr::Extract { field, expr: inner, .. } => Some(ScalarExpr::Extract {
            field: field.to_string(),
            expr: Box::new(lower_scalar(inner, params)?),
        }),
        _ => None,
    }
}

/// Evaluates a [`ScalarExpr`] against one row (column values in `col_names`
/// order). Unresolvable columns and type-invalid operations yield `Null`.
pub(crate) fn eval_scalar_expr(expr: &ScalarExpr, row: &[Value], col_names: &[String]) -> Value {
    match expr {
        ScalarExpr::Literal(v) => v.clone(),
        ScalarExpr::Column(name) => {
            let direct = col_names
                .iter()
                .position(|c| c == name || c.ends_with(&format!(".{name}")))
                .and_then(|i| row.get(i))
                .cloned();
            match direct {
                Some(v) => v,
                // A JSON access (`col->>'k'` / `col->'k'`) encoded as a
                // synthetic column name: compute it per row.
                None => match crate::filter_eval::parse_json_ref(name) {
                    Some((base, op, key)) => col_names
                        .iter()
                        .position(|c| c == &base || c.ends_with(&format!(".{base}")))
                        .and_then(|i| row.get(i))
                        .map(|v| crate::filter_eval::json_extract(v, &op, &key))
                        .unwrap_or(Value::Null),
                    None => Value::Null,
                },
            }
        }
        ScalarExpr::Unary { op, expr } => apply_unary_op(*op, eval_scalar_expr(expr, row, col_names)),
        ScalarExpr::Binary { op, left, right } => apply_binary_op(
            *op,
            eval_scalar_expr(left, row, col_names),
            eval_scalar_expr(right, row, col_names),
        ),
        ScalarExpr::Cast { expr, target } => {
            cast_value(eval_scalar_expr(expr, row, col_names), target)
        }
        ScalarExpr::Function { name, args } => {
            let vals: Vec<Value> = args
                .iter()
                .map(|a| eval_scalar_expr(a, row, col_names))
                .collect();
            eval_scalar_function(name, &vals)
        }
        ScalarExpr::IsNull { expr, negated } => {
            let is_null = matches!(eval_scalar_expr(expr, row, col_names), Value::Null);
            Value::Bool(if *negated { !is_null } else { is_null })
        }
        ScalarExpr::Extract { field, expr } => {
            extract_datetime_field(&eval_scalar_expr(expr, row, col_names), field)
        }
        // Aggregates are only meaningful over a group; per-row eval yields NULL.
        ScalarExpr::Aggregate { .. } => Value::Null,
        ScalarExpr::DateOffset {
            base,
            months,
            days,
            seconds,
        } => apply_date_offset(
            &eval_scalar_expr(base, row, col_names),
            *months,
            *days,
            *seconds,
        ),
        ScalarExpr::Case {
            operand,
            branches,
            else_result,
        } => {
            let op_val = operand.as_ref().map(|o| eval_scalar_expr(o, row, col_names));
            for (cond, result) in branches {
                let cond_val = eval_scalar_expr(cond, row, col_names);
                let hit = match &op_val {
                    // Simple CASE: operand = condition value (NULL never matches).
                    Some(ov) => ov != &Value::Null && crate::values_equal(ov, &cond_val),
                    // Searched CASE: condition must be boolean true.
                    None => cond_val == Value::Bool(true),
                };
                if hit {
                    return eval_scalar_expr(result, row, col_names);
                }
            }
            match else_result {
                Some(e) => eval_scalar_expr(e, row, col_names),
                None => Value::Null,
            }
        }
    }
}

/// Renders a `(months, days, seconds)` interval as canonical PostgreSQL text,
/// e.g. `1 year 2 mons 3 days` / `02:00:00`.
fn format_interval(months: i64, days: i64, seconds: i64) -> String {
    let mut parts = Vec::new();
    let plural = |n: i64, unit: &str| format!("{n} {unit}{}", if n.abs() == 1 { "" } else { "s" });
    let (years, mons) = (months / 12, months % 12);
    if years != 0 {
        parts.push(plural(years, "year"));
    }
    if mons != 0 {
        parts.push(plural(mons, "mon"));
    }
    if days != 0 {
        parts.push(plural(days, "day"));
    }
    if seconds != 0 {
        let s = seconds.abs();
        parts.push(format!(
            "{}{:02}:{:02}:{:02}",
            if seconds < 0 { "-" } else { "" },
            s / 3600,
            (s % 3600) / 60,
            s % 60
        ));
    }
    if parts.is_empty() {
        "00:00:00".to_string()
    } else {
        parts.join(" ")
    }
}

/// Parses an `INTERVAL` expression into a `(months, days, seconds)` offset.
/// Handles `INTERVAL '1 day'`, `INTERVAL '2 months 3 days'`, and
/// `INTERVAL '1' DAY` (value + leading field).
fn parse_interval(iv: &sqlparser::ast::Interval) -> Option<(i64, i64, i64)> {
    use sqlparser::ast::{Expr, Value as SqlValue};
    let raw = match &*iv.value {
        Expr::Value(v) => match &v.value {
            SqlValue::SingleQuotedString(s) => s.clone(),
            SqlValue::Number(n, _) => n.clone(),
            _ => return None,
        },
        _ => return None,
    };
    let (mut months, mut days, mut seconds) = (0i64, 0i64, 0i64);
    let tokens: Vec<&str> = raw.split_whitespace().collect();
    if tokens.len() >= 2 {
        let mut i = 0;
        while i + 1 < tokens.len() {
            let amount: i64 = tokens[i].parse().ok()?;
            apply_interval_unit(&mut months, &mut days, &mut seconds, amount, tokens[i + 1])?;
            i += 2;
        }
        return Some((months, days, seconds));
    }
    // Single amount with a leading field, e.g. `INTERVAL '1' DAY`.
    let amount: i64 = raw.trim().parse().ok()?;
    let unit = iv.leading_field.as_ref()?.to_string();
    apply_interval_unit(&mut months, &mut days, &mut seconds, amount, &unit)?;
    Some((months, days, seconds))
}

fn apply_interval_unit(
    months: &mut i64,
    days: &mut i64,
    seconds: &mut i64,
    amount: i64,
    unit: &str,
) -> Option<()> {
    match unit.to_ascii_lowercase().trim_end_matches('s') {
        "year" | "yr" => *months += amount * 12,
        // `mon`/`mons` is PostgreSQL's own rendering, so round-tripping needs it.
        "month" | "mon" => *months += amount,
        "week" => *days += amount * 7,
        "day" => *days += amount,
        "hour" => *seconds += amount * 3600,
        "minute" | "min" => *seconds += amount * 60,
        "second" | "sec" => *seconds += amount,
        _ => return None,
    }
    Some(())
}

/// Strictly parses interval *text* (`2 mons 3 days`, `1 year`, `02:00:00`,
/// `-5 days`) into `(months, days, seconds)`. Returns `None` for anything not
/// clearly an interval (bare numbers, dates, arbitrary text) so it never
/// hijacks ordinary text arithmetic or comparison.
pub(crate) fn parse_interval_text(s: &str) -> Option<(i64, i64, i64)> {
    let tokens: Vec<&str> = s.trim().split_whitespace().collect();
    if tokens.is_empty() {
        return None;
    }
    let (mut months, mut days, mut seconds) = (0i64, 0i64, 0i64);
    let mut i = 0;
    let mut matched = false;
    while i < tokens.len() {
        if let Some(secs) = parse_hms_token(tokens[i]) {
            seconds += secs;
            matched = true;
            i += 1;
            continue;
        }
        if i + 1 < tokens.len()
            && let Ok(amount) = tokens[i].parse::<i64>()
            && apply_interval_unit(&mut months, &mut days, &mut seconds, amount, tokens[i + 1])
                .is_some()
        {
            matched = true;
            i += 2;
            continue;
        }
        return None; // an unrecognized token means this isn't an interval
    }
    matched.then_some((months, days, seconds))
}

/// Parses an `HH:MM:SS` (optionally negative) interval time component to seconds.
fn parse_hms_token(t: &str) -> Option<i64> {
    let (neg, body) = t.strip_prefix('-').map_or((false, t), |r| (true, r));
    let parts: Vec<&str> = body.split(':').collect();
    if parts.len() != 3 {
        return None;
    }
    let h: i64 = parts[0].parse().ok()?;
    let m: i64 = parts[1].parse().ok()?;
    let s: i64 = parts[2].parse().ok()?;
    let total = h * 3600 + m * 60 + s;
    Some(if neg { -total } else { total })
}

/// A comparable magnitude for an interval (PostgreSQL uses a 30-day month).
fn interval_total((months, days, seconds): (i64, i64, i64)) -> i64 {
    months * 30 * 86400 + days * 86400 + seconds
}

/// Compares two values, treating two interval-formatted texts by magnitude
/// (so `10 days` > `2 days`); otherwise defers to the generic `compare`.
pub(crate) fn interval_aware_compare(a: &Value, b: &Value) -> std::cmp::Ordering {
    if let (Value::Text(ls), Value::Text(rs)) = (a, b)
        && let (Some(li), Some(ri)) = (parse_interval_text(ls), parse_interval_text(rs))
    {
        return interval_total(li).cmp(&interval_total(ri));
    }
    compare(a, b)
}

/// `interval ± interval` and `date/timestamp ± interval` on text operands.
fn interval_date_arith(op: ScalarBinaryOp, l: &Value, r: &Value) -> Value {
    if !matches!(op, ScalarBinaryOp::Add | ScalarBinaryOp::Sub) {
        return Value::Null;
    }
    let (Value::Text(ls), Value::Text(rs)) = (l, r) else {
        return Value::Null;
    };
    let sign = if matches!(op, ScalarBinaryOp::Sub) { -1 } else { 1 };
    // interval ± interval
    if let (Some((m1, d1, s1)), Some((m2, d2, s2))) =
        (parse_interval_text(ls), parse_interval_text(rs))
    {
        return Value::Text(format_interval(
            m1 + sign * m2,
            d1 + sign * d2,
            s1 + sign * s2,
        ));
    }
    // date/timestamp ± interval (left is the date, right is the interval)
    if let Some((m, d, s)) = parse_interval_text(rs) {
        return apply_date_offset(l, sign * m, sign * d, sign * s);
    }
    Value::Null
}

/// Applies a `(months, days, seconds)` offset to an ISO date/timestamp text
/// value using real calendar math. Returns a date when the input was a date and
/// no sub-day offset applies, otherwise a timestamp.
pub(crate) fn apply_date_offset(v: &Value, months: i64, days: i64, seconds: i64) -> Value {
    use chrono::{Duration, Months, NaiveDate, NaiveDateTime};
    let add_months = |dt: NaiveDateTime, m: i64| -> NaiveDateTime {
        if m >= 0 {
            dt.checked_add_months(Months::new(m as u32)).unwrap_or(dt)
        } else {
            dt.checked_sub_months(Months::new((-m) as u32)).unwrap_or(dt)
        }
    };
    let text = match v {
        Value::Null => return Value::Null,
        Value::Text(s) => s.trim().to_string(),
        other => render(other),
    };
    if let Ok(dt) = NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S") {
        let shifted =
            add_months(dt, months) + Duration::days(days) + Duration::seconds(seconds);
        return Value::Text(shifted.format("%Y-%m-%d %H:%M:%S").to_string());
    }
    if let Ok(d) = NaiveDate::parse_from_str(&text, "%Y-%m-%d") {
        let base = d.and_hms_opt(0, 0, 0).unwrap();
        let shifted =
            add_months(base, months) + Duration::days(days) + Duration::seconds(seconds);
        return if seconds != 0 {
            Value::Text(shifted.format("%Y-%m-%d %H:%M:%S").to_string())
        } else {
            Value::Text(shifted.date().format("%Y-%m-%d").to_string())
        };
    }
    Value::Null
}

/// Maps an aggregate function name to its [`AggregateOp`].
fn aggregate_op(name: &str) -> Option<AggregateOp> {
    match name {
        "COUNT" => Some(AggregateOp::Count),
        "SUM" => Some(AggregateOp::Sum),
        "MIN" => Some(AggregateOp::Min),
        "MAX" => Some(AggregateOp::Max),
        "AVG" => Some(AggregateOp::Avg),
        _ => None,
    }
}

/// True if a scalar expression contains an aggregate call, so a query using it
/// must go through the grouping/aggregation path.
pub(crate) fn scalar_has_aggregate(expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::Aggregate { .. } => true,
        ScalarExpr::Unary { expr, .. }
        | ScalarExpr::Cast { expr, .. }
        | ScalarExpr::IsNull { expr, .. }
        | ScalarExpr::Extract { expr, .. } => scalar_has_aggregate(expr),
        ScalarExpr::DateOffset { base, .. } => scalar_has_aggregate(base),
        ScalarExpr::Binary { left, right, .. } => {
            scalar_has_aggregate(left) || scalar_has_aggregate(right)
        }
        ScalarExpr::Function { args, .. } => args.iter().any(scalar_has_aggregate),
        ScalarExpr::Case {
            operand,
            branches,
            else_result,
        } => {
            operand.as_deref().is_some_and(scalar_has_aggregate)
                || branches
                    .iter()
                    .any(|(c, r)| scalar_has_aggregate(c) || scalar_has_aggregate(r))
                || else_result.as_deref().is_some_and(scalar_has_aggregate)
        }
        ScalarExpr::Literal(_) | ScalarExpr::Column(_) => false,
    }
}

/// Applies a unary operator to a value; type-invalid combinations yield `Null`.
pub(crate) fn apply_unary_op(op: ScalarUnaryOp, v: Value) -> Value {
    match (op, v) {
        (ScalarUnaryOp::Neg, Value::Int(i)) => Value::Int(-i),
        (ScalarUnaryOp::Neg, Value::Float(f)) => Value::Float(-f),
        (ScalarUnaryOp::Not, Value::Bool(b)) => Value::Bool(!b),
        (_, Value::Null) => Value::Null,
        _ => Value::Null,
    }
}

/// Applies a binary operator to two values with SQL NULL propagation and
/// three-valued logic; type-invalid combinations yield `Null`.
pub(crate) fn apply_binary_op(op: ScalarBinaryOp, l: Value, r: Value) -> Value {
    use ScalarBinaryOp as Op;
    let as_f64 = |v: &Value| -> Option<f64> {
        match v {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        }
    };
    match op {
        Op::Add | Op::Sub | Op::Mul | Op::Div | Op::Mod => {
            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                return Value::Null;
            }
            if let (Value::Int(a), Value::Int(b)) = (&l, &r) {
                let out = match op {
                    Op::Add => a.checked_add(*b),
                    Op::Sub => a.checked_sub(*b),
                    Op::Mul => a.checked_mul(*b),
                    Op::Div if *b != 0 => Some(a / b),
                    Op::Mod if *b != 0 => Some(a % b),
                    Op::Div | Op::Mod => return Value::Null, // division by zero
                    _ => return Value::Null,
                };
                out.map(Value::Int).unwrap_or(Value::Null)
            } else if let (Some(a), Some(b)) = (as_f64(&l), as_f64(&r)) {
                match op {
                    Op::Add => Value::Float(a + b),
                    Op::Sub => Value::Float(a - b),
                    Op::Mul => Value::Float(a * b),
                    Op::Div if b != 0.0 => Value::Float(a / b),
                    Op::Mod if b != 0.0 => Value::Float(a % b),
                    _ => Value::Null,
                }
            } else {
                // Non-numeric operands: interval/date arithmetic on text.
                interval_date_arith(op, &l, &r)
            }
        }
        Op::Eq | Op::NotEq | Op::Lt | Op::LtEq | Op::Gt | Op::GtEq => {
            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                return Value::Null;
            }
            use std::cmp::Ordering::{Equal, Greater, Less};
            let ord = interval_aware_compare(&l, &r);
            Value::Bool(match op {
                Op::Eq => ord == Equal,
                Op::NotEq => ord != Equal,
                Op::Lt => ord == Less,
                Op::LtEq => ord != Greater,
                Op::Gt => ord == Greater,
                Op::GtEq => ord != Less,
                _ => return Value::Null,
            })
        }
        Op::Concat => {
            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                Value::Null
            } else {
                Value::Text(format!("{}{}", render(&l), render(&r)))
            }
        }
        Op::And | Op::Or => {
            let lb = match l {
                Value::Bool(b) => Some(b),
                Value::Null => None,
                _ => return Value::Null,
            };
            let rb = match r {
                Value::Bool(b) => Some(b),
                Value::Null => None,
                _ => return Value::Null,
            };
            match op {
                Op::And => match (lb, rb) {
                    (Some(false), _) | (_, Some(false)) => Value::Bool(false),
                    (Some(true), Some(true)) => Value::Bool(true),
                    _ => Value::Null,
                },
                Op::Or => match (lb, rb) {
                    (Some(true), _) | (_, Some(true)) => Value::Bool(true),
                    (Some(false), Some(false)) => Value::Bool(false),
                    _ => Value::Null,
                },
                _ => Value::Null,
            }
        }
    }
}

/// Extracts a window/scalar function's arguments as strings: column names via
/// `extract_col_name`, or numeric/string literals (e.g. the LAG/LEAD offset).
pub(crate) fn window_args(func: &sqlparser::ast::Function) -> Vec<String> {
    use sqlparser::ast::{
        Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Value as SqlValue,
    };
    let args = match &func.args {
        FunctionArguments::List(list) => list.args.as_slice(),
        _ => &[],
    };
    args.iter()
        .filter_map(|a| match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                extract_col_name(e).or_else(|| match e {
                    Expr::Value(v) => match &v.value {
                        SqlValue::Number(n, _) => Some(n.clone()),
                        SqlValue::SingleQuotedString(s) => Some(s.clone()),
                        _ => None,
                    },
                    _ => None,
                })
            }
            _ => None,
        })
        .collect()
}

/// Lowers a sqlparser window frame (`ROWS`/`RANGE BETWEEN …`) into the plan
/// representation. A shorthand `ROWS n PRECEDING` (no `BETWEEN`) has an
/// implicit `AND CURRENT ROW` end bound.
pub(crate) fn window_frame(
    spec: &sqlparser::ast::WindowSpec,
) -> Option<crate::plan_types::WindowFrame> {
    use crate::plan_types::{WindowBound, WindowFrame, WindowFrameUnits};
    use sqlparser::ast::WindowFrameUnits as AstUnits;
    let frame = spec.window_frame.as_ref()?;
    let units = match frame.units {
        AstUnits::Rows => WindowFrameUnits::Rows,
        // GROUPS is treated as RANGE (peer-based) for our purposes.
        AstUnits::Range | AstUnits::Groups => WindowFrameUnits::Range,
    };
    let start = lower_bound(&frame.start_bound);
    let end = frame
        .end_bound
        .as_ref()
        .map(lower_bound)
        .unwrap_or(WindowBound::CurrentRow);
    Some(WindowFrame { units, start, end })
}

fn lower_bound(b: &sqlparser::ast::WindowFrameBound) -> crate::plan_types::WindowBound {
    use crate::plan_types::WindowBound;
    use sqlparser::ast::WindowFrameBound as B;
    // Extract a small integer literal from a bound offset expression.
    let as_int = |e: &Option<Box<sqlparser::ast::Expr>>| -> Option<i64> {
        match e.as_deref()? {
            sqlparser::ast::Expr::Value(v) => match &v.value {
                sqlparser::ast::Value::Number(n, _) => n.parse().ok(),
                _ => None,
            },
            _ => None,
        }
    };
    match b {
        B::CurrentRow => WindowBound::CurrentRow,
        B::Preceding(e) => match as_int(e) {
            Some(n) => WindowBound::Preceding(n),
            None => WindowBound::UnboundedPreceding,
        },
        B::Following(e) => match as_int(e) {
            Some(n) => WindowBound::Following(n),
            None => WindowBound::UnboundedFollowing,
        },
    }
}
