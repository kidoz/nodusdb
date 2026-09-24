//! Expression-level parsing: SQL value/operand extraction and column-name resolution.
use super::*;
use crate::*;
use anyhow::Result;
use nodus_catalog::TableConstraint;

pub fn expr_to_value(expr: &sqlparser::ast::Expr, params: &[crate::Value]) -> Option<crate::Value> {
    use sqlparser::ast::{Expr, Value as SqlValue};
    match expr {
        Expr::Value(v) => match &v.value {
            SqlValue::SingleQuotedString(s)
            | SqlValue::EscapedStringLiteral(s)
            | SqlValue::UnicodeStringLiteral(s)
            | SqlValue::NationalStringLiteral(s) => Some(crate::Value::Text(s.clone())),
            SqlValue::DollarQuotedString(s) => Some(crate::Value::Text(s.value.clone())),
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
        // Typed string literals like `DATE '2024-06-15'` / `TIMESTAMP '...'`,
        // in the type's canonical text. NodusDB has no native temporal type, so
        // dates stay ISO-8601 text (which compares and sorts chronologically).
        // Invalid input is `None`; the scalar path reports it when evaluated.
        Expr::TypedString(ts) => match &ts.value.value {
            SqlValue::SingleQuotedString(s) => {
                try_cast(crate::Value::Text(s.clone()), &ts.data_type.to_string()).ok()
            }
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
        // PostgreSQL JSON access (`->`/`->>`/`#>`/`#>>`) on a column parses as a
        // binary op. Nested access is left to the scalar evaluator.
        Expr::BinaryOp { left, op, right }
            if matches!(
                op,
                sqlparser::ast::BinaryOperator::Arrow
                    | sqlparser::ast::BinaryOperator::LongArrow
                    | sqlparser::ast::BinaryOperator::HashArrow
                    | sqlparser::ast::BinaryOperator::HashLongArrow
            ) && matches!(&**left, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) =>
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
/// Input that is invalid for the target type fails the statement.
pub(crate) fn cast_value(v: Value, data_type: &str) -> Value {
    try_cast(v, data_type).unwrap_or_else(crate::eval_error::raise)
}

/// [`cast_value`] without failing the statement: `Err` describes the invalid
/// input, for callers that fall back to keeping the original value.
pub(crate) fn try_cast(v: Value, data_type: &str) -> std::result::Result<Value, String> {
    use crate::value::ColumnType;
    if matches!(v, Value::Null) {
        return Ok(Value::Null);
    }
    if let Some(element_type) = crate::value::array_element_type(data_type) {
        return match v {
            Value::Array(items) => items
                .into_iter()
                .map(|item| match item {
                    Value::Array(_) => try_cast(item, data_type),
                    item => try_cast(item, element_type),
                })
                .collect::<std::result::Result<_, _>>()
                .map(Value::Array),
            Value::Text(s) => crate::value::coerce_array_text(&s, data_type)
                .ok_or_else(|| format!("malformed array literal: \"{s}\"")),
            other => Err(format!(
                "cannot cast {} to {data_type}",
                crate::value::value_type_name(&other)
            )),
        };
    }
    let invalid = |text: &str| {
        format!(
            "invalid input syntax for type {}: \"{text}\"",
            crate::value::sql_type_name(data_type)
        )
    };
    // PostgreSQL 18 casts a JSON scalar to a number or boolean; JSON null is NULL.
    let v = match v {
        Value::Jsonb(serde_json::Value::Null) => return Ok(Value::Null),
        Value::Jsonb(serde_json::Value::Number(n))
            if !matches!(crate::value::column_type(data_type), ColumnType::Text) =>
        {
            Value::Text(n.to_string())
        }
        Value::Jsonb(serde_json::Value::Bool(b))
            if matches!(crate::value::column_type(data_type), ColumnType::Bool) =>
        {
            Value::Bool(b)
        }
        other => other,
    };
    Ok(match crate::value::column_type(data_type) {
        // PostgreSQL rounds half-to-even when casting a number to an integer;
        // integer text must be a whole number.
        ColumnType::Int => match &v {
            Value::Int(_) => v,
            Value::Float(f) if f.is_finite() => Value::Int(f.round_ties_even() as i64),
            Value::Float(_) => {
                return Err(format!(
                    "{} out of range",
                    crate::value::sql_type_name(data_type)
                ));
            }
            Value::Bool(b) => Value::Int(i64::from(*b)),
            Value::Text(s) => s
                .trim()
                .parse::<i64>()
                .map(Value::Int)
                .map_err(|_| invalid(s))?,
            other => return Err(invalid(&render(other))),
        },
        ColumnType::Float => match &v {
            Value::Float(_) => v,
            Value::Int(i) => Value::Float(*i as f64),
            Value::Bool(b) => Value::Float(if *b { 1.0 } else { 0.0 }),
            Value::Text(s) => s
                .trim()
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| invalid(s))?,
            other => return Err(invalid(&render(other))),
        },
        ColumnType::Bool => match &v {
            Value::Bool(_) => v,
            Value::Int(i) => Value::Bool(*i != 0),
            Value::Text(s) => match parse_bool_text(s) {
                Value::Null => return Err(invalid(s)),
                b => b,
            },
            other => return Err(invalid(&render(other))),
        },
        ColumnType::Text => {
            let upper = data_type.trim().to_ascii_uppercase();
            match &v {
                // Booleans cast to the SQL spellings, not the wire `t`/`f` rendering.
                Value::Bool(b) => Value::Text(if *b { "true" } else { "false" }.to_string()),
                Value::Text(s) if upper == "JSON" || upper == "JSONB" => {
                    serde_json::from_str::<serde_json::Value>(s).map_err(|_| invalid(s))?;
                    v
                }
                Value::Text(s) if let Some(kind) = crate::value::temporal_type(data_type) => {
                    match crate::value::normalize_temporal(s, kind) {
                        Some(text) => Value::Text(text),
                        // Well-formed but impossible (`2024-02-30`, `25:00`).
                        None if crate::value::looks_temporal(s) => {
                            return Err(format!("date/time field value out of range: \"{s}\""));
                        }
                        None => return Err(invalid(s)),
                    }
                }
                Value::Text(s) if upper == "UUID" => Value::Text(
                    uuid::Uuid::parse_str(s.trim())
                        .map_err(|_| invalid(s))?
                        .hyphenated()
                        .to_string(),
                ),
                _ => Value::Text(render(&v)),
            }
        }
    })
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
pub(crate) fn parse_bool_text(s: &str) -> Value {
    match s.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "y" | "yes" | "on" | "1" => Value::Bool(true),
        "f" | "false" | "n" | "no" | "off" | "0" => Value::Bool(false),
        _ => Value::Null,
    }
}

/// Maps a SQL binary operator to the scalar evaluator's operator, including the
/// schema-qualified `OPERATOR(pg_catalog.=)` spelling; `None` if unsupported.
fn scalar_binary_op(op: &sqlparser::ast::BinaryOperator) -> Option<ScalarBinaryOp> {
    use sqlparser::ast::BinaryOperator as B;
    Some(match op {
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
        B::Arrow => ScalarBinaryOp::JsonGet,
        B::LongArrow => ScalarBinaryOp::JsonGetText,
        B::HashArrow => ScalarBinaryOp::JsonPath,
        B::HashLongArrow => ScalarBinaryOp::JsonPathText,
        B::Question => ScalarBinaryOp::JsonHasKey,
        B::QuestionPipe => ScalarBinaryOp::JsonHasAnyKey,
        B::QuestionAnd => ScalarBinaryOp::JsonHasAllKeys,
        B::AtArrow => ScalarBinaryOp::Contains,
        B::ArrowAt => ScalarBinaryOp::ContainedBy,
        B::PGOverlap => ScalarBinaryOp::Overlap,
        B::PGCustomBinaryOperator(parts) => match parts.last().map(String::as_str) {
            Some("=") => ScalarBinaryOp::Eq,
            Some("<>") => ScalarBinaryOp::NotEq,
            Some("<") => ScalarBinaryOp::Lt,
            Some("<=") => ScalarBinaryOp::LtEq,
            Some(">") => ScalarBinaryOp::Gt,
            Some(">=") => ScalarBinaryOp::GtEq,
            _ => return None,
        },
        B::Custom(s) if s == "@>" => ScalarBinaryOp::Contains,
        B::Custom(s) if s == "<@" => ScalarBinaryOp::ContainedBy,
        _ => return None,
    })
}

/// Recognizes the pattern-matching operators: POSIX regex (`~`, `~*`, `!~`,
/// `!~*`) and LIKE (`~~`, `~~*`, `!~~`, `!~~*`), bare or schema-qualified.
/// Returns `(kind, case_insensitive, negated)`.
fn pattern_operator(op: &sqlparser::ast::BinaryOperator) -> Option<(PatternKind, bool, bool)> {
    use sqlparser::ast::BinaryOperator as B;
    let symbol = match op {
        B::PGRegexMatch => "~",
        B::PGRegexIMatch => "~*",
        B::PGRegexNotMatch => "!~",
        B::PGRegexNotIMatch => "!~*",
        B::PGLikeMatch => "~~",
        B::PGILikeMatch => "~~*",
        B::PGNotLikeMatch => "!~~",
        B::PGNotILikeMatch => "!~~*",
        B::PGCustomBinaryOperator(parts) => parts.last()?.as_str(),
        _ => return None,
    };
    Some(match symbol {
        "~" => (PatternKind::Regex, false, false),
        "~*" => (PatternKind::Regex, true, false),
        "!~" => (PatternKind::Regex, false, true),
        "!~*" => (PatternKind::Regex, true, true),
        "~~" => (PatternKind::Like, false, false),
        "~~*" => (PatternKind::Like, true, false),
        "!~~" => (PatternKind::Like, false, true),
        "!~~*" => (PatternKind::Like, true, true),
        _ => return None,
    })
}

/// Lowers `expr [NOT] LIKE|ILIKE|SIMILAR TO pattern [ESCAPE e]`. The escape
/// defaults to backslash; `ESCAPE ''` disables it.
fn lower_pattern(
    expr: &sqlparser::ast::Expr,
    pattern: &sqlparser::ast::Expr,
    escape: Option<&sqlparser::ast::Expr>,
    (kind, case_insensitive, negated): (PatternKind, bool, bool),
    params: &[Value],
) -> Option<ScalarExpr> {
    let escape = match escape {
        None => Some('\\'),
        Some(e) => match expr_to_value(e, params)? {
            Value::Text(t) => {
                let mut chars = t.chars();
                match (chars.next(), chars.next()) {
                    (None, _) => None,
                    (Some(c), None) => Some(c),
                    _ => return None,
                }
            }
            _ => return None,
        },
    };
    Some(ScalarExpr::PatternMatch {
        expr: Box::new(lower_scalar(expr, params)?),
        pattern: Box::new(lower_scalar(pattern, params)?),
        kind,
        case_insensitive,
        negated,
        escape,
    })
}

/// Lowers a SQL scalar expression into a serializable [`ScalarExpr`] for
/// per-row evaluation in a table projection. Returns `None` for forms not yet
/// supported, so the planner can fall back to its existing handling.
pub(crate) fn lower_scalar(expr: &sqlparser::ast::Expr, params: &[Value]) -> Option<ScalarExpr> {
    use sqlparser::ast::{BinaryOperator as B, Expr, UnaryOperator as U};
    match expr {
        // A placeholder is unbound while a statement is planned for Describe,
        // which only needs the plan's shape; Execute binds the real value.
        Expr::Value(v) if matches!(v.value, sqlparser::ast::Value::Placeholder(_)) => Some(
            ScalarExpr::Literal(expr_to_value(expr, params).unwrap_or(Value::Null)),
        ),
        Expr::Value(_) | Expr::Interval(_) => expr_to_value(expr, params).map(ScalarExpr::Literal),
        // A typed literal is a cast of its text, so invalid input fails when
        // the statement runs, with the cast's error.
        Expr::TypedString(ts) => match (&ts.value.value, expr_to_value(expr, params)) {
            (_, Some(value)) => Some(ScalarExpr::Literal(value)),
            (sqlparser::ast::Value::SingleQuotedString(s), None) => Some(ScalarExpr::Cast {
                expr: Box::new(ScalarExpr::Literal(Value::Text(s.clone()))),
                target: ts.data_type.to_string(),
            }),
            _ => None,
        },
        // `ARRAY[...]`: a constant when every element is, else built per row.
        Expr::Array(array) => {
            let items = array
                .elem
                .iter()
                .map(|e| lower_scalar(e, params))
                .collect::<Option<Vec<_>>>()?;
            let constants = items
                .iter()
                .map(|item| match item {
                    ScalarExpr::Literal(value) => Some(value.clone()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>();
            Some(match constants {
                Some(values) => ScalarExpr::Literal(Value::Array(values)),
                None => ScalarExpr::Function {
                    name: "ARRAY".to_string(),
                    args: items,
                },
            })
        }
        // SQL keywords that read like identifiers but call a function.
        Expr::Identifier(id)
            if id.quote_style.is_none() && is_keyword_function(&id.value.to_ascii_uppercase()) =>
        {
            Some(ScalarExpr::Function {
                name: id.value.to_ascii_uppercase(),
                args: Vec::new(),
            })
        }
        Expr::Identifier(id) => Some(ScalarExpr::Column(id.value.clone())),
        Expr::CompoundIdentifier(ids) => Some(ScalarExpr::Column(
            ids.iter()
                .map(|id| id.value.clone())
                .collect::<Vec<_>>()
                .join("."),
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
            if let Some((kind, case_insensitive, negated)) = pattern_operator(op) {
                return Some(ScalarExpr::PatternMatch {
                    expr: Box::new(lower_scalar(left, params)?),
                    pattern: Box::new(lower_scalar(right, params)?),
                    kind,
                    case_insensitive,
                    negated,
                    // `~~` (LIKE) uses LIKE's default backslash escape.
                    escape: (kind == PatternKind::Like).then_some('\\'),
                });
            }
            let op = scalar_binary_op(op)?;
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
        Expr::Collate { expr: inner, .. } => lower_scalar(inner, params),
        Expr::Like {
            negated,
            any: false,
            expr: inner,
            pattern,
            escape_char,
        } => lower_pattern(
            inner,
            pattern,
            escape_char.as_deref(),
            (PatternKind::Like, false, *negated),
            params,
        ),
        Expr::ILike {
            negated,
            any: false,
            expr: inner,
            pattern,
            escape_char,
        } => lower_pattern(
            inner,
            pattern,
            escape_char.as_deref(),
            (PatternKind::Like, true, *negated),
            params,
        ),
        Expr::SimilarTo {
            negated,
            expr: inner,
            pattern,
            escape_char,
        } => lower_pattern(
            inner,
            pattern,
            escape_char.as_deref(),
            (PatternKind::SimilarTo, false, *negated),
            params,
        ),
        Expr::IsDistinctFrom(l, r) | Expr::IsNotDistinctFrom(l, r) => {
            Some(ScalarExpr::IsDistinctFrom {
                left: Box::new(lower_scalar(l, params)?),
                right: Box::new(lower_scalar(r, params)?),
                negated: matches!(expr, Expr::IsNotDistinctFrom(..)),
            })
        }
        Expr::IsTrue(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotUnknown(inner) => {
            let (value, negated) = match expr {
                Expr::IsTrue(_) => (Some(true), false),
                Expr::IsNotTrue(_) => (Some(true), true),
                Expr::IsFalse(_) => (Some(false), false),
                Expr::IsNotFalse(_) => (Some(false), true),
                Expr::IsUnknown(_) => (None, false),
                _ => (None, true),
            };
            Some(ScalarExpr::IsBool {
                expr: Box::new(lower_scalar(inner, params)?),
                value,
                negated,
            })
        }
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => Some(ScalarExpr::InList {
            expr: Box::new(lower_scalar(inner, params)?),
            list: list
                .iter()
                .map(|e| lower_scalar(e, params))
                .collect::<Option<_>>()?,
            negated: *negated,
        }),
        // `x BETWEEN a AND b` is `x >= a AND x <= b`; NOT BETWEEN is `x < a OR x > b`.
        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => {
            let x = lower_scalar(inner, params)?;
            let (lo_op, hi_op, join) = if *negated {
                (ScalarBinaryOp::Lt, ScalarBinaryOp::Gt, ScalarBinaryOp::Or)
            } else {
                (
                    ScalarBinaryOp::GtEq,
                    ScalarBinaryOp::LtEq,
                    ScalarBinaryOp::And,
                )
            };
            let bound = |op, e: &Expr| {
                Some(ScalarExpr::Binary {
                    op,
                    left: Box::new(x.clone()),
                    right: Box::new(lower_scalar(e, params)?),
                })
            };
            Some(ScalarExpr::Binary {
                op: join,
                left: Box::new(bound(lo_op, low)?),
                right: Box::new(bound(hi_op, high)?),
            })
        }
        // `x <op> ANY|ALL (array)`; the subquery forms are filters, not scalars.
        Expr::AnyOp {
            left,
            compare_op,
            right,
            ..
        }
        | Expr::AllOp {
            left,
            compare_op,
            right,
        } if !matches!(&**right, Expr::Subquery(_)) => {
            let op = scalar_binary_op(compare_op)?;
            if !matches!(
                op,
                ScalarBinaryOp::Eq
                    | ScalarBinaryOp::NotEq
                    | ScalarBinaryOp::Lt
                    | ScalarBinaryOp::LtEq
                    | ScalarBinaryOp::Gt
                    | ScalarBinaryOp::GtEq
            ) {
                return None;
            }
            Some(ScalarExpr::Quantified {
                left: Box::new(lower_scalar(left, params)?),
                op,
                right: Box::new(lower_scalar(right, params)?),
                all: matches!(expr, Expr::AllOp { .. }),
            })
        }
        Expr::Tuple(items) => Some(ScalarExpr::Row(
            items
                .iter()
                .map(|e| lower_scalar(e, params))
                .collect::<Option<_>>()?,
        )),
        Expr::Function(func) => {
            use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
            let name = func.name.to_string().to_uppercase();
            // Built-ins may be schema-qualified (`pg_catalog.lower(x)`).
            let name = name
                .strip_prefix("PG_CATALOG.")
                .map_or(name.clone(), str::to_string);
            // `ROW(a, b, ...)` is a row constructor, like a bare `(a, b, ...)`.
            if name == "ROW"
                && let FunctionArguments::List(list) = &func.args
            {
                let mut items = Vec::with_capacity(list.args.len());
                for a in &list.args {
                    match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                            items.push(lower_scalar(e, params)?)
                        }
                        _ => return None,
                    }
                }
                return Some(ScalarExpr::Row(items));
            }
            // Window calls and aggregate modifiers have their own paths.
            if func.over.is_some() || func.filter.is_some() || !func.within_group.is_empty() {
                return None;
            }
            // An aggregate nested in an expression, e.g. `sum(a) + 1`.
            if let Some(op) = aggregate_op(&name) {
                if matches!(&func.args, FunctionArguments::List(list) if !list.clauses.is_empty()) {
                    return None;
                }
                let FunctionArguments::List(list) = &func.args else {
                    return None;
                };
                let distinct = matches!(
                    list.duplicate_treatment,
                    Some(sqlparser::ast::DuplicateTreatment::Distinct)
                );
                return match list.args.first() {
                    Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) => {
                        Some(ScalarExpr::Aggregate {
                            op,
                            arg: "*".to_string(),
                            arg_expr: None,
                            distinct,
                        })
                    }
                    Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => {
                        match extract_col_name(e) {
                            Some(col) => Some(ScalarExpr::Aggregate {
                                op,
                                arg: col,
                                arg_expr: None,
                                distinct,
                            }),
                            // Aggregate over a computed expression, e.g. `sum(a + 1)`.
                            None => Some(ScalarExpr::Aggregate {
                                op,
                                arg: String::new(),
                                arg_expr: Some(Box::new(lower_scalar(e, params)?)),
                                distinct,
                            }),
                        }
                    }
                    _ => None,
                };
            }
            if !crate::functions::is_known(&name) {
                return None;
            }
            let args = match &func.args {
                // Keyword functions such as `current_user` take no parentheses.
                FunctionArguments::None => Vec::new(),
                FunctionArguments::List(list) if list.clauses.is_empty() => {
                    let mut args = Vec::with_capacity(list.args.len());
                    for a in &list.args {
                        match a {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                                args.push(lower_scalar(e, params)?)
                            }
                            _ => return None,
                        }
                    }
                    args
                }
                _ => return None,
            };
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
            trim_what,
            trim_characters,
        } => {
            use sqlparser::ast::TrimWhereField;
            let name = match trim_where {
                Some(TrimWhereField::Leading) => "LTRIM",
                Some(TrimWhereField::Trailing) => "RTRIM",
                _ => "BTRIM",
            };
            let mut args = vec![lower_scalar(inner, params)?];
            // `TRIM(chars FROM s)` or `TRIM(s, chars)`: the set of characters.
            match (trim_what, trim_characters.as_deref()) {
                (Some(chars), _) => args.push(lower_scalar(chars, params)?),
                (None, Some([chars])) => args.push(lower_scalar(chars, params)?),
                (None, None) => {}
                (None, Some(_)) => return None,
            }
            Some(ScalarExpr::Function {
                name: name.to_string(),
                args,
            })
        }
        Expr::Position { expr: sub, r#in } => Some(ScalarExpr::Function {
            name: "STRPOS".to_string(),
            args: vec![lower_scalar(r#in, params)?, lower_scalar(sub, params)?],
        }),
        Expr::Overlay {
            expr: inner,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            let mut args = vec![
                lower_scalar(inner, params)?,
                lower_scalar(overlay_what, params)?,
                lower_scalar(overlay_from, params)?,
            ];
            if let Some(len) = overlay_for {
                args.push(lower_scalar(len, params)?);
            }
            Some(ScalarExpr::Function {
                name: "OVERLAY".to_string(),
                args,
            })
        }
        Expr::Ceil { expr: inner, field } | Expr::Floor { expr: inner, field }
            if matches!(
                field,
                sqlparser::ast::CeilFloorKind::DateTimeField(
                    sqlparser::ast::DateTimeField::NoDateTime
                )
            ) =>
        {
            Some(ScalarExpr::Function {
                name: if matches!(expr, Expr::Ceil { .. }) {
                    "CEIL"
                } else {
                    "FLOOR"
                }
                .to_string(),
                args: vec![lower_scalar(inner, params)?],
            })
        }
        Expr::Extract {
            field, expr: inner, ..
        } => Some(ScalarExpr::Extract {
            field: field.to_string(),
            expr: Box::new(lower_scalar(inner, params)?),
        }),
        _ => None,
    }
}

/// Resolves the leaves of a [`ScalarExpr`] whose value depends on where it is
/// evaluated: column references and aggregate calls.
pub(crate) trait ScalarScope {
    fn column(&self, name: &str) -> Value;
    /// An aggregate over the scope's rows; a single row has none, so NULL.
    fn aggregate(&self, _expr: &ScalarExpr) -> Value {
        Value::Null
    }
}

/// One row, with column values in `col_names` order.
struct RowScope<'a> {
    row: &'a [Value],
    col_names: &'a [String],
}

impl ScalarScope for RowScope<'_> {
    fn column(&self, name: &str) -> Value {
        let direct = crate::filter_eval::col_pos(self.col_names, name)
            .and_then(|i| self.row.get(i))
            .cloned();
        match direct {
            Some(v) => v,
            // A JSON access (`col->>'k'` / `col->'k'`) encoded as a
            // synthetic column name: compute it per row.
            None => match crate::filter_eval::parse_json_ref(name) {
                Some((base, op, key)) => self
                    .col_names
                    .iter()
                    .position(|c| c == &base || c.ends_with(&format!(".{base}")))
                    .and_then(|i| self.row.get(i))
                    .map(|v| crate::filter_eval::json_extract(v, &op, &key))
                    .unwrap_or(Value::Null),
                None => Value::Null,
            },
        }
    }
}

/// Evaluates a [`ScalarExpr`] against one row (column values in `col_names`
/// order). Unresolvable columns and type-invalid operations yield `Null`.
pub(crate) fn eval_scalar_expr(expr: &ScalarExpr, row: &[Value], col_names: &[String]) -> Value {
    eval_scalar_in(expr, &RowScope { row, col_names })
}

/// Evaluates a [`ScalarExpr`] with SQL NULL propagation and three-valued logic,
/// resolving columns and aggregates through `scope`.
pub(crate) fn eval_scalar_in(expr: &ScalarExpr, scope: &dyn ScalarScope) -> Value {
    let eval = |e: &ScalarExpr| eval_scalar_in(e, scope);
    match expr {
        ScalarExpr::Literal(v) => v.clone(),
        ScalarExpr::Column(name) => scope.column(name),
        ScalarExpr::Aggregate { .. } => scope.aggregate(expr),
        ScalarExpr::Unary { op, expr } => apply_unary_op(*op, eval(expr)),
        ScalarExpr::Binary { op, left, right } => eval_comparison(*op, left, right, scope),
        ScalarExpr::Cast { expr, target } => cast_value(eval(expr), target),
        ScalarExpr::Function { name, args } => {
            let vals: Vec<Value> = args.iter().map(eval).collect();
            crate::functions::call(name, &vals)
        }
        ScalarExpr::IsNull { expr, negated } => {
            let is_null = matches!(eval(expr), Value::Null);
            Value::Bool(if *negated { !is_null } else { is_null })
        }
        ScalarExpr::Extract { field, expr } => extract_datetime_field(&eval(expr), field),
        ScalarExpr::DateOffset {
            base,
            months,
            days,
            seconds,
        } => apply_date_offset(&eval(base), *months, *days, *seconds),
        ScalarExpr::Case {
            operand,
            branches,
            else_result,
        } => {
            let op_val = operand.as_ref().map(|o| eval(o));
            for (cond, result) in branches {
                let cond_val = eval(cond);
                let hit = match &op_val {
                    // Simple CASE: operand = condition value (NULL never matches).
                    Some(ov) => ov != &Value::Null && crate::values_equal(ov, &cond_val),
                    // Searched CASE: condition must be boolean true.
                    None => cond_val == Value::Bool(true),
                };
                if hit {
                    return eval(result);
                }
            }
            match else_result {
                Some(e) => eval(e),
                None => Value::Null,
            }
        }
        ScalarExpr::PatternMatch {
            expr,
            pattern,
            kind,
            case_insensitive,
            negated,
            escape,
        } => pattern_match(
            &eval(expr),
            &eval(pattern),
            *kind,
            *case_insensitive,
            *escape,
        )
        .map_or(Value::Null, |hit| Value::Bool(hit != *negated)),
        ScalarExpr::IsDistinctFrom {
            left,
            right,
            negated,
        } => {
            let distinct = match eval_comparison(ScalarBinaryOp::NotEq, left, right, scope) {
                Value::Bool(b) => b,
                // NULL vs NULL is not distinct; NULL vs a value is.
                _ => !(matches!(eval(left), Value::Null) && matches!(eval(right), Value::Null)),
            };
            Value::Bool(distinct != *negated)
        }
        ScalarExpr::IsBool {
            expr,
            value,
            negated,
        } => {
            let v = eval(expr);
            let hit = match value {
                Some(b) => v == Value::Bool(*b),
                None => v == Value::Null,
            };
            Value::Bool(hit != *negated)
        }
        ScalarExpr::InList {
            expr,
            list,
            negated,
        } => {
            // True on any match; otherwise NULL if any comparison was unknown.
            let mut unknown = false;
            let mut found = false;
            for item in list {
                match eval_comparison(ScalarBinaryOp::Eq, expr, item, scope) {
                    Value::Bool(true) => {
                        found = true;
                        break;
                    }
                    Value::Bool(false) => {}
                    _ => unknown = true,
                }
            }
            let result = if found {
                Value::Bool(true)
            } else if unknown {
                Value::Null
            } else {
                Value::Bool(false)
            };
            if *negated {
                apply_unary_op(ScalarUnaryOp::Not, result)
            } else {
                result
            }
        }
        ScalarExpr::Quantified {
            left,
            op,
            right,
            all,
        } => quantified_compare(*op, eval(left), eval(right), *all),
        ScalarExpr::Row(items) => {
            let rendered: Vec<String> = items.iter().map(|e| render(&eval(e))).collect();
            Value::Text(format!("({})", rendered.join(",")))
        }
    }
}

/// Applies a binary operator, comparing row constructors element-wise.
fn eval_comparison(
    op: ScalarBinaryOp,
    left: &ScalarExpr,
    right: &ScalarExpr,
    scope: &dyn ScalarScope,
) -> Value {
    if let (ScalarExpr::Row(l), ScalarExpr::Row(r)) = (left, right)
        && is_comparison(op)
    {
        return compare_rows(op, l, r, scope);
    }
    apply_binary_op(
        op,
        eval_scalar_in(left, scope),
        eval_scalar_in(right, scope),
    )
}

fn is_comparison(op: ScalarBinaryOp) -> bool {
    use ScalarBinaryOp as Op;
    matches!(
        op,
        Op::Eq | Op::NotEq | Op::Lt | Op::LtEq | Op::Gt | Op::GtEq
    )
}

/// PostgreSQL row comparison: `=`/`<>` hold only if every pair decides it, and
/// ordering is decided by the first unequal pair; a NULL on the way is unknown.
fn compare_rows(
    op: ScalarBinaryOp,
    left: &[ScalarExpr],
    right: &[ScalarExpr],
    scope: &dyn ScalarScope,
) -> Value {
    use ScalarBinaryOp as Op;
    if left.len() != right.len() {
        return Value::Null;
    }
    if matches!(op, Op::Eq | Op::NotEq) {
        let mut unknown = false;
        for (l, r) in left.iter().zip(right) {
            match eval_comparison(Op::Eq, l, r, scope) {
                Value::Bool(true) => {}
                Value::Bool(false) => return Value::Bool(op == Op::NotEq),
                _ => unknown = true,
            }
        }
        return if unknown {
            Value::Null
        } else {
            Value::Bool(op == Op::Eq)
        };
    }
    for (l, r) in left.iter().zip(right) {
        match eval_comparison(Op::Eq, l, r, scope) {
            Value::Bool(true) => continue,
            Value::Bool(false) => {
                let strict = match op {
                    Op::LtEq => Op::Lt,
                    Op::GtEq => Op::Gt,
                    other => other,
                };
                return eval_comparison(strict, l, r, scope);
            }
            _ => return Value::Null,
        }
    }
    Value::Bool(matches!(op, Op::LtEq | Op::GtEq))
}

/// `left <op> ANY|ALL (array)`: ANY is true on any true comparison, ALL false
/// on any false one; otherwise an unknown comparison makes the result NULL.
/// Array text (`'{1,2}'`) is accepted as the array operand.
pub(crate) fn quantified_compare(
    op: ScalarBinaryOp,
    left: Value,
    right: Value,
    all: bool,
) -> Value {
    let items = match right {
        Value::Array(items) => items,
        Value::Text(s) => match crate::value::parse_array_literal(&s) {
            Some(items) => items,
            None => return Value::Null,
        },
        _ => return Value::Null,
    };
    fn flatten(items: Vec<Value>, out: &mut Vec<Value>) {
        for item in items {
            match item {
                Value::Array(inner) => flatten(inner, out),
                v => out.push(v),
            }
        }
    }
    let mut flat = Vec::new();
    flatten(items, &mut flat);
    let mut unknown = false;
    for item in flat {
        match apply_binary_op(op, left.clone(), item) {
            Value::Bool(b) if b != all => return Value::Bool(b),
            Value::Bool(_) => {}
            _ => unknown = true,
        }
    }
    if unknown {
        Value::Null
    } else {
        Value::Bool(all)
    }
}

/// Matches `value` against a LIKE, SIMILAR TO, or POSIX regex pattern; `None`
/// when either side is NULL or the pattern is invalid.
fn pattern_match(
    value: &Value,
    pattern: &Value,
    kind: PatternKind,
    case_insensitive: bool,
    escape: Option<char>,
) -> Option<bool> {
    if matches!(value, Value::Null) || matches!(pattern, Value::Null) {
        return None;
    }
    let pattern = render(pattern);
    let source = match kind {
        PatternKind::Like => like_to_regex(&pattern, escape)?,
        PatternKind::SimilarTo => similar_to_regex(&pattern, escape)?,
        PatternKind::Regex => pattern,
    };
    let source = if case_insensitive {
        format!("(?i){source}")
    } else {
        source
    };
    cached_regex(&source).map(|re| re.is_match(&render(value)))
}

/// Translates a LIKE pattern into an anchored regex: `%` is any run, `_` one
/// character, and the escape character makes the next character literal.
fn like_to_regex(pattern: &str, escape: Option<char>) -> Option<String> {
    let mut out = String::from("(?s)^");
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if Some(c) == escape {
            // A trailing escape character is invalid, as in PostgreSQL.
            out.push_str(&regex::escape(&chars.next()?.to_string()));
            continue;
        }
        match c {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            c => out.push_str(&regex::escape(&c.to_string())),
        }
    }
    out.push('$');
    Some(out)
}

/// Translates a SQL `SIMILAR TO` pattern into an anchored regex: LIKE's `%` and
/// `_`, plus the SQL regex operators `| * + ? {m,n} ( ) [...]`; any other
/// character (including `.`) is literal.
fn similar_to_regex(pattern: &str, escape: Option<char>) -> Option<String> {
    let mut out = String::from("(?s)^(?:");
    let mut chars = pattern.chars();
    let mut in_class = false;
    while let Some(c) = chars.next() {
        if Some(c) == escape {
            out.push_str(&regex::escape(&chars.next()?.to_string()));
            continue;
        }
        if in_class {
            in_class = c != ']';
            out.push(c);
            continue;
        }
        match c {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            '[' => {
                in_class = true;
                out.push(c);
            }
            '|' | '*' | '+' | '?' | '{' | '}' | '(' | ')' => out.push(c),
            c => out.push_str(&regex::escape(&c.to_string())),
        }
    }
    out.push_str(")$");
    Some(out)
}

/// Compiles `source` once per thread; patterns are usually per-statement
/// constants, so rows reuse the compiled regex. `None` for an invalid pattern.
fn cached_regex(source: &str) -> Option<regex::Regex> {
    use std::cell::RefCell;
    use std::collections::HashMap;
    thread_local! {
        static CACHE: RefCell<HashMap<String, Option<regex::Regex>>> = RefCell::new(HashMap::new());
    }
    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(re) = cache.get(source) {
            return re.clone();
        }
        if cache.len() >= 256 {
            cache.clear();
        }
        let re = regex::Regex::new(source).ok();
        cache.insert(source.to_string(), re.clone());
        re
    })
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
    let bare_date = |text: &str| {
        crate::value::parse_temporal(text)
            .filter(|t| t.time.is_none())
            .map(|t| t.date)
    };
    // date ± integer days, integer + date, and date - date (whole days).
    match (l, r) {
        (Value::Text(date), Value::Int(days)) | (Value::Int(days), Value::Text(date))
            if bare_date(date).is_some()
                && (matches!(l, Value::Text(_)) || matches!(op, ScalarBinaryOp::Add)) =>
        {
            let days = if matches!(op, ScalarBinaryOp::Sub) {
                -days
            } else {
                *days
            };
            return bare_date(date)
                .and_then(|d| d.checked_add_signed(chrono::Duration::days(days)))
                .map(|d| Value::Text(d.format("%Y-%m-%d").to_string()))
                .unwrap_or_else(|| crate::eval_error::raise("date out of range"));
        }
        (Value::Text(a), Value::Text(b)) if matches!(op, ScalarBinaryOp::Sub) => {
            if let (Some(a), Some(b)) = (bare_date(a), bare_date(b)) {
                return Value::Int((a - b).num_days());
            }
        }
        _ => {}
    }
    let (Value::Text(ls), Value::Text(rs)) = (l, r) else {
        return Value::Null;
    };
    let sign = if matches!(op, ScalarBinaryOp::Sub) {
        -1
    } else {
        1
    };
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
            dt.checked_sub_months(Months::new((-m) as u32))
                .unwrap_or(dt)
        }
    };
    let text = match v {
        Value::Null => return Value::Null,
        Value::Text(s) => s.trim().to_string(),
        other => render(other),
    };
    let Some(parsed) = crate::value::parse_temporal(&text) else {
        return Value::Null;
    };
    let base = match parsed.offset {
        Some(_) => parsed.utc(),
        None => parsed.date.and_time(parsed.time.unwrap_or_default()),
    };
    let shifted = add_months(base, months) + Duration::days(days) + Duration::seconds(seconds);
    // A date shifted by whole days stays a date; otherwise the result is a
    // timestamp, zoned if the input was.
    if parsed.time.is_none() && seconds == 0 {
        Value::Text(shifted.date().format("%Y-%m-%d").to_string())
    } else {
        Value::Text(crate::value::format_timestamp(
            shifted,
            parsed.offset.is_some(),
        ))
    }
}

/// Keywords PostgreSQL evaluates as functions without parentheses.
fn is_keyword_function(upper: &str) -> bool {
    matches!(
        upper,
        "CURRENT_ROLE"
            | "CURRENT_USER"
            | "SESSION_USER"
            | "USER"
            | "CURRENT_CATALOG"
            | "CURRENT_SCHEMA"
            | "CURRENT_DATE"
            | "CURRENT_TIME"
            | "CURRENT_TIMESTAMP"
            | "LOCALTIME"
            | "LOCALTIMESTAMP"
    )
}

/// The error for the first call to a function NodusDB does not provide, if
/// an expression makes one: PostgreSQL's `function f(types) does not exist`.
pub(crate) fn unknown_function_error(expr: &sqlparser::ast::Expr) -> Option<String> {
    use sqlparser::ast::Expr;
    fn arg_exprs(func: &sqlparser::ast::Function) -> Vec<&sqlparser::ast::Expr> {
        use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
        match &func.args {
            FunctionArguments::List(list) => list
                .args
                .iter()
                .filter_map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } => Some(e),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        }
    }
    let children: Vec<&Expr> = match expr {
        Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            let name = name.strip_prefix("PG_CATALOG.").unwrap_or(&name);
            let args = arg_exprs(func);
            if name != "ROW"
                && aggregate_op(name).is_none()
                && func.over.is_none()
                && !crate::functions::is_known(name)
            {
                let types = args
                    .iter()
                    .map(|arg| literal_type_name(arg))
                    .collect::<Vec<_>>()
                    .join(", ");
                return Some(format!(
                    "function {}({types}) does not exist",
                    func.name.to_string().to_ascii_lowercase()
                ));
            }
            args
        }
        Expr::Nested(e)
        | Expr::UnaryOp { expr: e, .. }
        | Expr::Cast { expr: e, .. }
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsTrue(e)
        | Expr::IsFalse(e)
        | Expr::IsNotTrue(e)
        | Expr::IsNotFalse(e)
        | Expr::Collate { expr: e, .. } => vec![e],
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::AnyOp { left, right, .. }
        | Expr::AllOp { left, right, .. } => vec![left, right],
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => vec![expr, pattern],
        Expr::Between {
            expr, low, high, ..
        } => vec![expr, low, high],
        Expr::InList { expr, list, .. } => std::iter::once(&**expr).chain(list).collect(),
        Expr::Tuple(items) => items.iter().collect(),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => operand
            .iter()
            .map(|e| &**e)
            .chain(conditions.iter().flat_map(|w| [&w.condition, &w.result]))
            .chain(else_result.iter().map(|e| &**e))
            .collect(),
        _ => Vec::new(),
    };
    children.into_iter().find_map(unknown_function_error)
}

/// A best-effort PostgreSQL type name for a function argument in an error.
fn literal_type_name(expr: &sqlparser::ast::Expr) -> String {
    use sqlparser::ast::{Expr, Value as V};
    match expr {
        Expr::Value(v) => match &v.value {
            V::Number(n, _) if n.contains(['.', 'e', 'E']) => "numeric",
            V::Number(n, _) if n.parse::<i32>().is_ok() => "integer",
            V::Number(..) => "bigint",
            V::Boolean(_) => "boolean",
            _ => "unknown",
        }
        .to_string(),
        Expr::Cast { data_type, .. } => crate::value::sql_type_name(&data_type.to_string()),
        Expr::Nested(inner) => literal_type_name(inner),
        _ => "unknown".to_string(),
    }
}

/// Maps an aggregate function name to its [`AggregateOp`].
pub(crate) fn aggregate_op(name: &str) -> Option<AggregateOp> {
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
    matches!(expr, ScalarExpr::Aggregate { .. })
        || expr.children().into_iter().any(scalar_has_aggregate)
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
                    Op::Div | Op::Mod if *b == 0 => {
                        return crate::eval_error::raise("division by zero");
                    }
                    Op::Add => a.checked_add(*b),
                    Op::Sub => a.checked_sub(*b),
                    Op::Mul => a.checked_mul(*b),
                    Op::Div => a.checked_div(*b),
                    Op::Mod => a.checked_rem(*b),
                    _ => return Value::Null,
                };
                out.map(Value::Int)
                    .unwrap_or_else(|| crate::eval_error::raise("bigint out of range"))
            } else if let (Some(a), Some(b)) = (as_f64(&l), as_f64(&r)) {
                let out = match op {
                    Op::Div | Op::Mod if b == 0.0 => {
                        return crate::eval_error::raise("division by zero");
                    }
                    Op::Add => a + b,
                    Op::Sub => a - b,
                    Op::Mul => a * b,
                    Op::Div => a / b,
                    Op::Mod => a % b,
                    _ => return Value::Null,
                };
                if out.is_infinite() && a.is_finite() && b.is_finite() {
                    return crate::eval_error::raise("value out of range: overflow");
                }
                Value::Float(out)
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
            let (l, r) = unify_for_comparison(l, r);
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
            match (&l, &r) {
                // Array concatenation: array || array, array || element, element || array.
                (Value::Array(a), Value::Array(b)) => {
                    return Value::Array(a.iter().chain(b).cloned().collect());
                }
                (Value::Array(a), elem) => {
                    return Value::Array(a.iter().cloned().chain([elem.clone()]).collect());
                }
                (elem, Value::Array(b)) => {
                    return Value::Array(
                        [elem.clone()]
                            .into_iter()
                            .chain(b.iter().cloned())
                            .collect(),
                    );
                }
                _ => {}
            }
            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                Value::Null
            } else {
                Value::Text(format!("{}{}", render(&l), render(&r)))
            }
        }
        Op::JsonGet
        | Op::JsonGetText
        | Op::JsonPath
        | Op::JsonPathText
        | Op::JsonHasKey
        | Op::JsonHasAnyKey
        | Op::JsonHasAllKeys
        | Op::Contains
        | Op::ContainedBy
        | Op::Overlap => {
            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                return Value::Null;
            }
            apply_json_array_op(op, &l, &r)
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

/// Resolves an untyped text operand against a typed one, the way PostgreSQL
/// resolves an unknown-typed literal to the other operand's type (`5 = '5'`,
/// `flag = 't'`, `doc = '{"a":1}'`). Text that doesn't parse is left as is.
fn unify_for_comparison(l: Value, r: Value) -> (Value, Value) {
    fn resolve(text: &str, typed: &Value) -> Option<Value> {
        let t = text.trim();
        match typed {
            Value::Int(_) | Value::Float(_) => t
                .parse::<i64>()
                .map(Value::Int)
                .ok()
                .or_else(|| t.parse::<f64>().ok().map(Value::Float)),
            Value::Bool(_) => match parse_bool_text(t) {
                Value::Bool(b) => Some(Value::Bool(b)),
                _ => None,
            },
            Value::Jsonb(_) => serde_json::from_str(t).ok().map(Value::Jsonb),
            _ => None,
        }
    }
    match (&l, &r) {
        (Value::Text(t), typed) => match resolve(t, typed) {
            Some(v) => (v, r),
            None => (l, r),
        },
        (typed, Value::Text(t)) => match resolve(t, typed) {
            Some(v) => (l, v),
            None => (l, r),
        },
        _ => (l, r),
    }
}

/// JSON access/existence and JSONB/array containment operators over non-NULL
/// operands. A missing field, element, or path yields NULL.
fn apply_json_array_op(op: ScalarBinaryOp, l: &Value, r: &Value) -> Value {
    use ScalarBinaryOp as Op;
    use serde_json::Value as J;
    let as_value = |j: Option<J>, as_text: bool| match j {
        None => Value::Null,
        Some(J::Null) if as_text => Value::Null,
        Some(J::String(s)) if as_text => Value::Text(s),
        Some(j) if as_text => Value::Text(j.to_string()),
        Some(j) => Value::Jsonb(j),
    };
    let texts = |v: &Value| -> Vec<String> {
        let items = match v {
            Value::Array(items) => items.clone(),
            Value::Text(s) => crate::value::parse_array_literal(s).unwrap_or_default(),
            _ => Vec::new(),
        };
        items
            .iter()
            .filter(|i| !matches!(i, Value::Null))
            .map(render)
            .collect()
    };
    match op {
        Op::Contains => Value::Bool(crate::filter_eval::value_contains(l, r)),
        Op::ContainedBy => Value::Bool(crate::filter_eval::value_contains(r, l)),
        Op::Overlap => {
            let (a, b) = (texts(l), texts(r));
            Value::Bool(a.iter().any(|x| b.contains(x)))
        }
        _ => {
            let Some(json) = crate::filter_eval::value_to_json(l) else {
                return Value::Null;
            };
            match op {
                Op::JsonGet | Op::JsonGetText => {
                    as_value(json_step(&json, r), op == Op::JsonGetText)
                }
                Op::JsonPath | Op::JsonPathText => {
                    let mut cur = Some(json);
                    for key in texts(r) {
                        cur = cur.and_then(|j| json_step(&j, &Value::Text(key)));
                    }
                    as_value(cur, op == Op::JsonPathText)
                }
                Op::JsonHasKey | Op::JsonHasAnyKey | Op::JsonHasAllKeys => {
                    let has = |key: &str| match &json {
                        J::Object(map) => map.contains_key(key),
                        J::Array(items) => items.iter().any(|i| i.as_str() == Some(key)),
                        J::String(s) => s == key,
                        _ => false,
                    };
                    Value::Bool(match op {
                        Op::JsonHasKey => has(&render(r)),
                        Op::JsonHasAnyKey => texts(r).iter().any(|k| has(k)),
                        _ => texts(r).iter().all(|k| has(k)),
                    })
                }
                _ => Value::Null,
            }
        }
    }
}

/// One JSON access step: an object field by name, or an array element by
/// (possibly negative) integer index, given as an integer or numeric text.
fn json_step(json: &serde_json::Value, key: &Value) -> Option<serde_json::Value> {
    use serde_json::Value as J;
    match json {
        J::Object(map) => map.get(&render(key)).cloned(),
        J::Array(items) => {
            let idx = match key {
                Value::Int(i) => *i,
                Value::Text(t) => t.trim().parse::<i64>().ok()?,
                _ => return None,
            };
            let idx = if idx < 0 {
                items.len() as i64 + idx
            } else {
                idx
            };
            usize::try_from(idx)
                .ok()
                .and_then(|i| items.get(i))
                .cloned()
        }
        _ => None,
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
