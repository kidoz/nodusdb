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
            // An integer literal is an integer; one with a point or exponent (or
            // too large for bigint) is an exact numeric, as in PostgreSQL.
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Some(crate::Value::Int(i))
                } else if let Some(d) = crate::value::parse_decimal(n) {
                    Some(crate::Value::Numeric(d))
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
        // So is a value that depends on the session (a zoned timestamp is read
        // in its time zone, `now` and `today` at its transaction's start), as
        // the session is known only when the statement runs.
        Expr::TypedString(ts) => match &ts.value.value {
            SqlValue::SingleQuotedString(s)
                if crate::datetime::Kind::of_type(&ts.data_type.to_string())
                    != Some(crate::datetime::Kind::TimestampTz)
                    && !["now", "today", "tomorrow", "yesterday"]
                        .contains(&s.trim().to_ascii_lowercase().as_str()) =>
            {
                try_cast(crate::Value::Text(s.clone()), &ts.data_type.to_string()).ok()
            }
            _ => None,
        },
        // `INTERVAL '1 day'` — NodusDB has no native interval type, so it's kept
        // as canonical PostgreSQL text (round-trips through INTERVAL columns).
        Expr::Interval(iv) => parse_interval(iv).map(|iv| crate::Value::Text(iv.format())),
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
                    crate::Value::Numeric(d) => Some(crate::Value::Numeric(-d)),
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
    if let Some(kind) = crate::value::object_identifier_type(data_type) {
        return crate::MemExecutor::object_identifier(v, kind);
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
    let target = data_type.trim().to_ascii_uppercase();
    // `json` keeps its text as `json`, and is that text as anything else.
    let v = match v {
        Value::Json(text) if target == "JSON" => return Ok(Value::Json(text)),
        Value::Json(text) => Value::Text(text),
        Value::Record(_) => Value::Text(render(&v)),
        Value::Jsonb(j) if target == "JSON" => {
            return Ok(Value::Json(crate::json_text::jsonb_text(&j)));
        }
        other => other,
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
        // PostgreSQL rounds a float to an integer half-to-even and a numeric
        // half away from zero; the result must fit the integer's width.
        ColumnType::Int => {
            let out_of_range =
                || format!("{} out of range", crate::value::sql_type_name(data_type));
            let n = match &v {
                Value::Int(i) => *i,
                Value::Float(f) if f.is_finite() && f.abs() < 9.3e18 => f.round_ties_even() as i64,
                Value::Float(_) => return Err(out_of_range()),
                Value::Numeric(d) => {
                    use rust_decimal::prelude::ToPrimitive;
                    d.round_dp_with_strategy(
                        0,
                        rust_decimal::RoundingStrategy::MidpointAwayFromZero,
                    )
                    .to_i64()
                    .ok_or_else(out_of_range)?
                }
                Value::Bool(b) => i64::from(*b),
                Value::Text(s) => s.trim().parse::<i64>().map_err(|_| invalid(s))?,
                other => return Err(invalid(&render(other))),
            };
            let (min, max) = crate::value::integer_range(data_type);
            if !(min..=max).contains(&n) {
                return Err(out_of_range());
            }
            Value::Int(n)
        }
        ColumnType::Numeric => {
            let d = match &v {
                Value::Numeric(d) => *d,
                Value::Int(i) => rust_decimal::Decimal::from(*i),
                Value::Float(f) if f.is_finite() => crate::value::parse_decimal(&f.to_string())
                    .ok_or_else(|| "value overflows numeric format".to_string())?,
                Value::Float(_) => return Err("cannot convert infinity or NaN to numeric".into()),
                Value::Text(s) => crate::value::parse_decimal(s).ok_or_else(|| invalid(s))?,
                other => return Err(invalid(&render(other))),
            };
            Value::Numeric(crate::value::apply_numeric_typmod(d, data_type)?)
        }
        ColumnType::Float => match &v {
            Value::Float(_) => v,
            Value::Numeric(d) => Value::Float(crate::value::decimal_to_f64(d)),
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
                // `json` keeps its text; `jsonb` is the parsed document.
                Value::Text(s) if upper == "JSON" => {
                    crate::json_text::parse(s)?;
                    Value::Json(s.clone())
                }
                Value::Text(s) if upper == "JSONB" => Value::Jsonb(crate::json_text::parse(s)?),
                Value::Text(s)
                    if crate::datetime::Kind::of_type(data_type)
                        == Some(crate::datetime::Kind::Interval) =>
                {
                    match crate::datetime::Interval::parse(s) {
                        Some(interval) => Value::Text(interval.format()),
                        None => {
                            return Err(format!("invalid input syntax for type interval: \"{s}\""));
                        }
                    }
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
        Expr::Value(_) => expr_to_value(expr, params).map(ScalarExpr::Literal),
        // An interval literal keeps its type, so operators on it know it.
        Expr::Interval(iv) => Some(ScalarExpr::Cast {
            expr: Box::new(ScalarExpr::Literal(Value::Text(
                parse_interval(iv)?.format(),
            ))),
            target: "INTERVAL".to_string(),
        }),
        // A typed literal is a cast of its text: it keeps its type, and
        // invalid input fails when the statement runs, with the cast's error.
        Expr::TypedString(ts) => match &ts.value.value {
            sqlparser::ast::Value::SingleQuotedString(s) => Some(ScalarExpr::Cast {
                expr: Box::new(ScalarExpr::Literal(Value::Text(s.clone()))),
                target: ts.data_type.to_string(),
            }),
            _ => expr_to_value(expr, params).map(ScalarExpr::Literal),
        },
        // `ts AT TIME ZONE zone` is `timezone(zone, ts)`.
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => Some(ScalarExpr::Function {
            name: "TIMEZONE".to_string(),
            args: vec![
                lower_scalar(time_zone, params)?,
                lower_scalar(timestamp, params)?,
            ],
        }),
        // `base[i]`, `base[lo:hi]`, after any field names that complete a
        // column reference (`t.col[1]`).
        Expr::CompoundFieldAccess { root, access_chain } => {
            use sqlparser::ast::{AccessExpr, Subscript};
            let mut names: Vec<String> = match &**root {
                Expr::Identifier(id) => vec![id.value.clone()],
                Expr::CompoundIdentifier(ids) => ids.iter().map(|i| i.value.clone()).collect(),
                _ => Vec::new(),
            };
            let mut chain = access_chain.iter().peekable();
            while let (false, Some(AccessExpr::Dot(Expr::Identifier(field)))) =
                (names.is_empty(), chain.peek())
            {
                names.push(field.value.clone());
                chain.next();
            }
            let mut base = if names.is_empty() {
                lower_scalar(root, params)?
            } else {
                ScalarExpr::Column(names.join("."))
            };
            let bound = |e: &Option<Expr>| match e {
                Some(e) => lower_scalar(e, params),
                None => Some(ScalarExpr::Literal(Value::Null)),
            };
            for access in chain {
                let (name, args) = match access {
                    AccessExpr::Subscript(Subscript::Index { index }) => {
                        ("__SUBSCRIPT__", vec![base, lower_scalar(index, params)?])
                    }
                    AccessExpr::Subscript(Subscript::Slice {
                        lower_bound,
                        upper_bound,
                        stride: None,
                    }) => (
                        "__SLICE__",
                        vec![base, bound(lower_bound)?, bound(upper_bound)?],
                    ),
                    _ => return None,
                };
                base = ScalarExpr::Function {
                    name: name.to_string(),
                    args,
                };
            }
            Some(base)
        }
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
            // `(start, end) OVERLAPS (start, end)`.
            if *op == sqlparser::ast::BinaryOperator::Overlaps
                && let (Expr::Tuple(l), Expr::Tuple(r)) = (left.as_ref(), right.as_ref())
                && l.len() == 2
                && r.len() == 2
            {
                let args = l
                    .iter()
                    .chain(r)
                    .map(|e| lower_scalar(e, params))
                    .collect::<Option<Vec<_>>>()?;
                return Some(ScalarExpr::Function {
                    name: "OVERLAPS".to_string(),
                    args,
                });
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
        } => {
            let inner = lower_scalar(inner, params)?;
            let target = data_type.to_string();
            // An object identifier as text is the object's name
            // (`c.oid::regclass::text`).
            if let ScalarExpr::Cast { target: kind, .. } = &inner
                && let Some(kind) = crate::value::object_identifier_type(kind)
                && matches!(
                    target
                        .to_ascii_uppercase()
                        .trim_start_matches("PG_CATALOG."),
                    "TEXT" | "VARCHAR" | "NAME"
                )
            {
                return Some(ScalarExpr::Function {
                    name: "__OBJECT_NAME__".to_string(),
                    args: vec![inner, ScalarExpr::Literal(Value::Text(kind.to_string()))],
                });
            }
            Some(ScalarExpr::Cast {
                expr: Box::new(inner),
                target,
            })
        }
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
        // `x <op> ANY|ALL (array)` and `x <op> ANY|ALL (SELECT ...)`.
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
        } => {
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
            let right = match &**right {
                Expr::Subquery(query) => subquery_expr(query, SubqueryKind::Array, params)?,
                other => lower_scalar(other, params)?,
            };
            Some(ScalarExpr::Quantified {
                left: Box::new(lower_scalar(left, params)?),
                op,
                right: Box::new(right),
                all: matches!(expr, Expr::AllOp { .. }),
            })
        }
        // Subqueries used as values, run by the executor for each row.
        Expr::Subquery(query) => subquery_expr(query, SubqueryKind::Scalar, params),
        Expr::Exists { subquery, negated } => {
            let exists = subquery_expr(subquery, SubqueryKind::Exists, params)?;
            Some(if *negated {
                ScalarExpr::Unary {
                    op: ScalarUnaryOp::Not,
                    expr: Box::new(exists),
                }
            } else {
                exists
            })
        }
        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => {
            let any = ScalarExpr::Quantified {
                left: Box::new(lower_scalar(inner, params)?),
                op: ScalarBinaryOp::Eq,
                right: Box::new(subquery_expr(subquery, SubqueryKind::Array, params)?),
                all: false,
            };
            Some(if *negated {
                ScalarExpr::Unary {
                    op: ScalarUnaryOp::Not,
                    expr: Box::new(any),
                }
            } else {
                any
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
            // `ARRAY(SELECT ...)`: the subquery's first column as an array.
            if name == "ARRAY"
                && let FunctionArguments::Subquery(query) = &func.args
            {
                return subquery_expr(query, SubqueryKind::Array, params);
            }
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
            if func.over.is_some() {
                return lower_window(func, &name, params);
            }
            // Ordered-set aggregates (`WITHIN GROUP`) and `IGNORE NULLS` are
            // not supported.
            if !func.within_group.is_empty() || func.null_treatment.is_some() {
                return None;
            }
            // An aggregate call, possibly nested in an expression (`sum(a) + 1`),
            // with optional DISTINCT, ORDER BY, and FILTER.
            if let Some(op) = aggregate_op(&name) {
                let FunctionArguments::List(list) = &func.args else {
                    return None;
                };
                let distinct = matches!(
                    list.duplicate_treatment,
                    Some(sqlparser::ast::DuplicateTreatment::Distinct)
                );
                let mut order_by = Vec::new();
                for clause in &list.clauses {
                    let sqlparser::ast::FunctionArgumentClause::OrderBy(keys) = clause else {
                        return None;
                    };
                    for key in keys {
                        let ascending = match &key.options.sort {
                            None | Some(sqlparser::ast::OrderBySort::Asc) => true,
                            Some(sqlparser::ast::OrderBySort::Desc) => false,
                            Some(_) => return None,
                        };
                        if key.with_fill.is_some() {
                            return None;
                        }
                        order_by.push((
                            lower_scalar(&key.expr, params)?,
                            ascending,
                            key.options.nulls_first,
                        ));
                    }
                }
                let filter = match &func.filter {
                    Some(condition) => Some(Box::new(lower_scalar(condition, params)?)),
                    None => None,
                };
                let mut args = list.args.iter();
                let (arg, arg_expr) = match args.next() {
                    Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
                        if op == AggregateOp::Count =>
                    {
                        ("*".to_string(), None)
                    }
                    Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => {
                        match lower_scalar(e, params)? {
                            ScalarExpr::Column(col) => (col, None),
                            // Aggregate over a computed expression, e.g. `sum(a + 1)`.
                            other => (String::new(), Some(Box::new(json_arg(&name, other)))),
                        }
                    }
                    _ => return None,
                };
                let extra_args = args
                    .map(|a| match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                            lower_scalar(e, params).map(|e| json_arg(&name, e))
                        }
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()?;
                if extra_args.len() + 1 != op.arity() {
                    return None;
                }
                return Some(ScalarExpr::Aggregate {
                    op,
                    arg,
                    arg_expr,
                    distinct,
                    extra_args,
                    filter,
                    order_by,
                });
            }
            // FILTER applies only to aggregates.
            if func.filter.is_some() || !crate::functions::is_known(&name) {
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
                            // `make_interval(days => 3)`: a named argument in
                            // its place, the ones left out zero.
                            FunctionArg::Named {
                                arg: FunctionArgExpr::Expr(e),
                                ..
                            }
                            | FunctionArg::ExprNamed {
                                arg: FunctionArgExpr::Expr(e),
                                ..
                            } if name == "MAKE_INTERVAL" => {
                                const NAMES: [&str; 7] =
                                    ["years", "months", "weeks", "days", "hours", "mins", "secs"];
                                let arg_name = match a {
                                    FunctionArg::Named { name, .. } => name.value.clone(),
                                    FunctionArg::ExprNamed { name, .. } => extract_col_name(name)?,
                                    _ => return None,
                                };
                                let at = NAMES
                                    .iter()
                                    .position(|n| n.eq_ignore_ascii_case(&arg_name))?;
                                while args.len() <= at {
                                    args.push(ScalarExpr::Literal(Value::Int(0)));
                                }
                                args[at] = lower_scalar(e, params)?;
                            }
                            _ => return None,
                        }
                    }
                    args
                }
                _ => return None,
            };
            let args = args.into_iter().map(|a| json_arg(&name, a)).collect();
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
        // The executor replaces subqueries with their values before
        // evaluating; one left here is in a place that cannot run it.
        ScalarExpr::Subquery { .. } => {
            crate::eval_error::raise("a subquery is not supported in this position")
        }
        // The executor computes windows before evaluating; one left here is
        // in a clause that cannot have them.
        ScalarExpr::Window(_) => crate::eval_error::raise("window functions are not allowed here"),
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
        ScalarExpr::Extract { field, expr } => {
            crate::datetime::extract(field, &eval(expr), None, false)
                .unwrap_or_else(crate::eval_error::raise)
        }
        ScalarExpr::DateOffset {
            base,
            months,
            days,
            seconds,
        } => {
            let offset = crate::datetime::Interval::new(
                *months,
                *days,
                seconds * crate::datetime::MICROS_PER_SECOND,
            );
            crate::datetime::arith(
                "+",
                &eval(base),
                None,
                &Value::Text(offset.format()),
                Some(crate::datetime::Kind::Interval),
            )
            .unwrap_or_else(crate::eval_error::raise)
        }
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

/// An `INTERVAL` literal: `INTERVAL '1 day'`, `INTERVAL '2 months 3
/// days'`, or a number with its unit after (`INTERVAL '1' DAY`).
fn parse_interval(iv: &sqlparser::ast::Interval) -> Option<crate::datetime::Interval> {
    use sqlparser::ast::{Expr, Value as SqlValue};
    let raw = match &*iv.value {
        Expr::Value(v) => match &v.value {
            SqlValue::SingleQuotedString(s) => s.clone(),
            SqlValue::Number(n, _) => n.clone(),
            _ => return None,
        },
        _ => return None,
    };
    match (&iv.leading_field, raw.trim().parse::<f64>()) {
        (Some(unit), Ok(_)) => crate::datetime::Interval::parse(&format!("{raw} {unit}")),
        _ => crate::datetime::Interval::parse(&raw),
    }
}

/// Compares two values, texts that both read as intervals (`10 days`,
/// `02:00:00`) by their length; otherwise defers to the generic `compare`.
pub(crate) fn interval_aware_compare(a: &Value, b: &Value) -> std::cmp::Ordering {
    let interval = |text: &str| {
        // A bare number is text, not seconds.
        (text.contains(':') || text.chars().any(|c| c.is_ascii_alphabetic()))
            .then(|| crate::datetime::Interval::parse(text))
            .flatten()
            .filter(|_| crate::value::parse_temporal(text).is_none())
    };
    if let (Value::Text(ls), Value::Text(rs)) = (a, b)
        && let (Some(li), Some(ri)) = (interval(ls), interval(rs))
    {
        return li.span().cmp(&ri.span());
    }
    compare(a, b)
}

thread_local! {
    /// Why the last subquery in an expression could not be planned, so the
    /// statement reports that instead of a generic "unsupported expression".
    static SUBQUERY_ERROR: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

/// Plans a subquery used inside an expression; `None` (with the reason kept
/// for [`expression_error`]) if it cannot be planned.
fn subquery_expr(
    query: &sqlparser::ast::Query,
    kind: SubqueryKind,
    params: &[Value],
) -> Option<ScalarExpr> {
    match plan_query(query, params) {
        Ok(plan) => Some(ScalarExpr::Subquery {
            plan: SubPlan(Box::new(plan)),
            kind,
        }),
        Err(e) => {
            SUBQUERY_ERROR.with(|slot| *slot.borrow_mut() = Some(e.to_string()));
            None
        }
    }
}

/// The error for an expression that could not be planned: a failing
/// subquery's own error, an unknown function, or else `fallback`.
pub(crate) fn expression_error(
    expr: &sqlparser::ast::Expr,
    fallback: impl FnOnce() -> String,
) -> anyhow::Error {
    if let Some(message) = SUBQUERY_ERROR.with(|slot| slot.borrow_mut().take()) {
        return anyhow::anyhow!(message);
    }
    anyhow::anyhow!(unknown_function_error(expr).unwrap_or_else(fallback))
}

/// The argument of a call of the marker function `marker` (nodus_sql's
/// rewrites of syntax the parser lacks).
fn marker_argument<'a>(
    e: &'a sqlparser::ast::Expr,
    marker: &str,
) -> Option<&'a sqlparser::ast::Expr> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
    let Expr::Function(f) = e else { return None };
    let name = f.name.to_string().to_ascii_uppercase();
    if name.strip_prefix("PG_CATALOG.") != Some(marker) {
        return None;
    }
    match &f.args {
        FunctionArguments::List(list) => match list.args.as_slice() {
            [FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))] => Some(arg),
            _ => None,
        },
        _ => None,
    }
}

/// Keeps the reason an expression cannot be planned, for
/// [`expression_error`] to report.
fn plan_error(message: impl Into<String>) -> Option<ScalarExpr> {
    SUBQUERY_ERROR.with(|slot| *slot.borrow_mut() = Some(message.into()));
    None
}

thread_local! {
    /// The `WINDOW` clause of the select being planned.
    static NAMED_WINDOWS: std::cell::RefCell<Vec<sqlparser::ast::NamedWindowDefinition>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Restores the enclosing select's named windows when a select's planning ends.
pub(crate) struct NamedWindowsScope(Vec<sqlparser::ast::NamedWindowDefinition>);

impl Drop for NamedWindowsScope {
    fn drop(&mut self) {
        let previous = std::mem::take(&mut self.0);
        NAMED_WINDOWS.with(|slot| *slot.borrow_mut() = previous);
    }
}

/// Makes a select's `WINDOW` clause visible to the window calls planned
/// until the returned scope ends.
pub(crate) fn named_windows_scope(
    definitions: &[sqlparser::ast::NamedWindowDefinition],
) -> NamedWindowsScope {
    NamedWindowsScope(NAMED_WINDOWS.with(|slot| slot.replace(definitions.to_vec())))
}

/// The window a call's `OVER` means: its own specification, a named window,
/// or a named window refined with an `ORDER BY` and frame of its own.
fn resolve_window(over: &sqlparser::ast::WindowType) -> Result<sqlparser::ast::WindowSpec, String> {
    use sqlparser::ast::{NamedWindowExpr, WindowType};
    fn named(
        name: &sqlparser::ast::Ident,
        depth: usize,
    ) -> Result<sqlparser::ast::WindowSpec, String> {
        let definition = NAMED_WINDOWS.with(|slot| {
            slot.borrow()
                .iter()
                .find(|d| d.0.value.eq_ignore_ascii_case(&name.value))
                .map(|d| d.1.clone())
        });
        match definition {
            _ if depth > 16 => Err(format!("window \"{}\" does not exist", name.value)),
            Some(NamedWindowExpr::WindowSpec(spec)) => refine(spec, depth + 1),
            Some(NamedWindowExpr::NamedWindow(other)) => named(&other, depth + 1),
            None => Err(format!("window \"{}\" does not exist", name.value)),
        }
    }
    fn refine(
        spec: sqlparser::ast::WindowSpec,
        depth: usize,
    ) -> Result<sqlparser::ast::WindowSpec, String> {
        let Some(base_name) = spec.window_name.clone() else {
            return Ok(spec);
        };
        let base = named(&base_name, depth)?;
        let name = &base_name.value;
        // An `EXCLUDE` marker (see nodus_sql) is no partition key.
        let (markers, keys): (Vec<sqlparser::ast::Expr>, Vec<sqlparser::ast::Expr>) = spec
            .partition_by
            .iter()
            .cloned()
            .partition(|e| marker_argument(e, nodus_sql::EXCLUDE_MARKER).is_some());
        if !keys.is_empty() {
            return Err(format!(
                "cannot override PARTITION BY clause of window \"{name}\""
            ));
        }
        if !spec.order_by.is_empty() && !base.order_by.is_empty() {
            return Err(format!(
                "cannot override ORDER BY clause of window \"{name}\""
            ));
        }
        if base.window_frame.is_some() {
            return Err(format!(
                "cannot copy window \"{name}\" because it has a frame clause"
            ));
        }
        Ok(sqlparser::ast::WindowSpec {
            window_name: None,
            partition_by: base.partition_by.into_iter().chain(markers).collect(),
            order_by: if spec.order_by.is_empty() {
                base.order_by
            } else {
                spec.order_by
            },
            window_frame: spec.window_frame,
        })
    }
    match over {
        WindowType::WindowSpec(spec) => refine(spec.clone(), 0),
        WindowType::NamedWindow(name) => named(name, 0),
    }
}

/// The functions that are window functions only.
const WINDOW_FUNCTIONS: &[(&str, std::ops::RangeInclusive<usize>)] = &[
    ("ROW_NUMBER", 0..=0),
    ("RANK", 0..=0),
    ("DENSE_RANK", 0..=0),
    ("PERCENT_RANK", 0..=0),
    ("CUME_DIST", 0..=0),
    ("NTILE", 1..=1),
    ("LAG", 1..=3),
    ("LEAD", 1..=3),
    ("FIRST_VALUE", 1..=1),
    ("LAST_VALUE", 1..=1),
    ("NTH_VALUE", 2..=2),
];

/// Plans a window call: `func(args) [FILTER (WHERE ...)] OVER window`.
fn lower_window(
    func: &sqlparser::ast::Function,
    name: &str,
    params: &[Value],
) -> Option<ScalarExpr> {
    use crate::plan_types::{FrameBound, FrameExclude, FrameSpec, WindowCall, WindowFrameUnits};
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments, WindowFrameBound};
    let over = func.over.as_ref()?;
    let spec = match resolve_window(over) {
        Ok(spec) => spec,
        Err(message) => return plan_error(message),
    };
    let aggregate = aggregate_op(name);
    let arity = WINDOW_FUNCTIONS
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, a)| a.clone());
    if aggregate.is_none() && arity.is_none() {
        return plan_error(format!(
            "OVER specified, but {} is not a window function nor an aggregate function",
            name.to_ascii_lowercase()
        ));
    }
    if !func.within_group.is_empty() || func.null_treatment.is_some() {
        return plan_error(format!("{} is not implemented for window functions", func));
    }
    let mut args = Vec::new();
    match &func.args {
        FunctionArguments::None => {}
        FunctionArguments::List(list) => {
            if matches!(
                list.duplicate_treatment,
                Some(sqlparser::ast::DuplicateTreatment::Distinct)
            ) {
                return plan_error("DISTINCT is not implemented for window functions");
            }
            if !list.clauses.is_empty() {
                return plan_error("aggregate ORDER BY is not implemented for window functions");
            }
            for arg in &list.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                        if aggregate == Some(AggregateOp::Count) => {}
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                        args.push(lower_scalar(e, params)?)
                    }
                    _ => return None,
                }
            }
        }
        FunctionArguments::Subquery(_) => return None,
    }
    if let Some(arity) = &arity
        && !arity.contains(&args.len())
    {
        return plan_error(format!(
            "function {}() does not exist",
            name.to_ascii_lowercase()
        ));
    }
    let filter = match &func.filter {
        None => None,
        Some(_) if aggregate.is_none() => {
            return plan_error("FILTER is not implemented for non-aggregate window functions");
        }
        Some(condition) => Some(lower_scalar(condition, params)?),
    };
    // `EXCLUDE ...` arrives as a marker in PARTITION BY (see nodus_sql).
    let mut exclude = FrameExclude::NoOthers;
    let mut partition_by = Vec::new();
    for e in &spec.partition_by {
        match lower_scalar(e, params)? {
            ScalarExpr::Function { name, args } if name == nodus_sql::EXCLUDE_MARKER => {
                exclude = match args.first() {
                    Some(ScalarExpr::Literal(Value::Text(kind))) => match kind.as_str() {
                        "current row" => FrameExclude::CurrentRow,
                        "group" => FrameExclude::Group,
                        "ties" => FrameExclude::Ties,
                        _ => FrameExclude::NoOthers,
                    },
                    _ => FrameExclude::NoOthers,
                };
            }
            other => partition_by.push(other),
        }
    }
    let mut order_by = Vec::new();
    for key in &spec.order_by {
        let ascending = match &key.options.sort {
            None | Some(sqlparser::ast::OrderBySort::Asc) => true,
            Some(sqlparser::ast::OrderBySort::Desc) => false,
            Some(_) => return None,
        };
        order_by.push((
            lower_scalar(&key.expr, params)?,
            ascending,
            key.options.nulls_first,
        ));
    }
    let frame = match &spec.window_frame {
        None if exclude == FrameExclude::NoOthers => None,
        None => Some(FrameSpec {
            units: WindowFrameUnits::Range,
            start: FrameBound::UnboundedPreceding,
            end: FrameBound::CurrentRow,
            exclude,
        }),
        Some(frame) => {
            let bound = |b: &WindowFrameBound| -> Option<FrameBound> {
                Some(match b {
                    WindowFrameBound::CurrentRow => FrameBound::CurrentRow,
                    WindowFrameBound::Preceding(None) => FrameBound::UnboundedPreceding,
                    WindowFrameBound::Following(None) => FrameBound::UnboundedFollowing,
                    WindowFrameBound::Preceding(Some(e)) => {
                        FrameBound::Preceding(lower_scalar(e, params)?)
                    }
                    WindowFrameBound::Following(Some(e)) => {
                        FrameBound::Following(lower_scalar(e, params)?)
                    }
                })
            };
            let start = bound(&frame.start_bound)?;
            let end = match &frame.end_bound {
                Some(b) => bound(b)?,
                None => FrameBound::CurrentRow,
            };
            let rank = |b: &FrameBound| match b {
                FrameBound::UnboundedPreceding => 0,
                FrameBound::Preceding(_) => 1,
                FrameBound::CurrentRow => 2,
                FrameBound::Following(_) => 3,
                FrameBound::UnboundedFollowing => 4,
            };
            if matches!(start, FrameBound::UnboundedFollowing) {
                return plan_error("frame start cannot be UNBOUNDED FOLLOWING");
            }
            if matches!(end, FrameBound::UnboundedPreceding) {
                return plan_error("frame end cannot be UNBOUNDED PRECEDING");
            }
            if rank(&start) == 2 && rank(&end) == 1 {
                return plan_error("frame starting from current row cannot have preceding rows");
            }
            if rank(&start) == 3 && rank(&end) <= 2 {
                return plan_error(if rank(&end) == 2 {
                    "frame starting from following row cannot end with current row"
                } else {
                    "frame starting from following row cannot have preceding rows"
                });
            }
            let units = match frame.units {
                sqlparser::ast::WindowFrameUnits::Rows => WindowFrameUnits::Rows,
                sqlparser::ast::WindowFrameUnits::Range => WindowFrameUnits::Range,
                sqlparser::ast::WindowFrameUnits::Groups => WindowFrameUnits::Groups,
            };
            if units == WindowFrameUnits::Groups && order_by.is_empty() {
                return plan_error("GROUPS mode requires an ORDER BY clause");
            }
            Some(FrameSpec {
                units,
                start,
                end,
                exclude,
            })
        }
    };
    Some(ScalarExpr::Window(Box::new(WindowCall {
        func: name.to_string(),
        args,
        filter,
        partition_by,
        order_by,
        frame,
    })))
}

/// Forgets a subquery error left by an earlier statement.
pub(crate) fn reset_expression_errors() {
    SUBQUERY_ERROR.with(|slot| slot.borrow_mut().take());
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

/// An argument of function `name`: a row constructor given to a JSON
/// function is a row with fields `f1`, `f2`, ..., which the function writes
/// as an object.
fn json_arg(name: &str, arg: ScalarExpr) -> ScalarExpr {
    let json =
        name.starts_with("JSON") || name.starts_with("TO_JSON") || name.ends_with("_TO_JSON");
    match arg {
        ScalarExpr::Row(items) if json => ScalarExpr::Function {
            name: "__RECORD__".to_string(),
            args: items
                .into_iter()
                .enumerate()
                .flat_map(|(i, item)| {
                    [
                        ScalarExpr::Literal(Value::Text(format!("f{}", i + 1))),
                        item,
                    ]
                })
                .collect(),
        },
        other => other,
    }
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
            let arg_count = match &func.args {
                sqlparser::ast::FunctionArguments::List(list) => list.args.len(),
                _ => 0,
            };
            if func.filter.is_some() && aggregate_op(name).is_none() && func.over.is_none() {
                return Some(format!(
                    "FILTER specified, but {} is not an aggregate function",
                    func.name.to_string().to_ascii_lowercase()
                ));
            }
            if name == "MERGE_ACTION" {
                return Some(
                    "MERGE_ACTION() can only be used in the RETURNING list of a MERGE command"
                        .to_string(),
                );
            }
            let wrong_aggregate_arity =
                aggregate_op(name).is_some_and(|op| op.arity() != arg_count);
            if name != "ROW"
                && (wrong_aggregate_arity
                    || (aggregate_op(name).is_none()
                        && func.over.is_none()
                        && !crate::functions::is_known(name)))
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
        "STRING_AGG" => Some(AggregateOp::StringAgg),
        "ARRAY_AGG" => Some(AggregateOp::ArrayAgg),
        "BOOL_AND" | "EVERY" => Some(AggregateOp::BoolAnd),
        "BOOL_OR" => Some(AggregateOp::BoolOr),
        "JSON_AGG" => Some(AggregateOp::JsonAgg),
        "JSONB_AGG" => Some(AggregateOp::JsonbAgg),
        "JSON_OBJECT_AGG" => Some(AggregateOp::JsonObjectAgg),
        "JSONB_OBJECT_AGG" => Some(AggregateOp::JsonbObjectAgg),
        "STDDEV" | "STDDEV_SAMP" => Some(AggregateOp::StddevSamp),
        "STDDEV_POP" => Some(AggregateOp::StddevPop),
        "VARIANCE" | "VAR_SAMP" => Some(AggregateOp::VarSamp),
        "VAR_POP" => Some(AggregateOp::VarPop),
        "BIT_AND" => Some(AggregateOp::BitAnd),
        "BIT_OR" => Some(AggregateOp::BitOr),
        "BIT_XOR" => Some(AggregateOp::BitXor),
        "ANY_VALUE" => Some(AggregateOp::AnyValue),
        "CORR" => Some(AggregateOp::Corr),
        "COVAR_POP" => Some(AggregateOp::CovarPop),
        "COVAR_SAMP" => Some(AggregateOp::CovarSamp),
        "REGR_SLOPE" => Some(AggregateOp::RegrSlope),
        "REGR_INTERCEPT" => Some(AggregateOp::RegrIntercept),
        "REGR_COUNT" => Some(AggregateOp::RegrCount),
        "REGR_R2" => Some(AggregateOp::RegrR2),
        "REGR_AVGX" => Some(AggregateOp::RegrAvgX),
        "REGR_AVGY" => Some(AggregateOp::RegrAvgY),
        "REGR_SXX" => Some(AggregateOp::RegrSxx),
        "REGR_SYY" => Some(AggregateOp::RegrSyy),
        "REGR_SXY" => Some(AggregateOp::RegrSxy),
        _ => None,
    }
}

/// True if a scalar expression contains an aggregate call, so a query using it
/// must go through the grouping/aggregation path.
pub(crate) fn scalar_has_aggregate(expr: &ScalarExpr) -> bool {
    matches!(expr, ScalarExpr::Aggregate { .. })
        || expr.children().into_iter().any(scalar_has_aggregate)
}

/// Whether an expression calls a window function.
pub(crate) fn scalar_has_window(expr: &ScalarExpr) -> bool {
    matches!(expr, ScalarExpr::Window(_)) || expr.children().into_iter().any(scalar_has_window)
}

/// Exact `numeric` arithmetic. Division takes PostgreSQL's result scale (at
/// least 16 significant digits); overflow and division by zero fail.
fn numeric_arith(op: ScalarBinaryOp, a: rust_decimal::Decimal, b: rust_decimal::Decimal) -> Value {
    use ScalarBinaryOp as Op;
    let result = match op {
        Op::Add => a.checked_add(b),
        Op::Sub => a.checked_sub(b),
        Op::Mul => a.checked_mul(b),
        Op::Div | Op::Mod if b.is_zero() => return crate::eval_error::raise("division by zero"),
        Op::Div => return numeric_div(a, b),
        Op::Mod => a.checked_rem(b),
        _ => return Value::Null,
    };
    result.map_or_else(
        || crate::eval_error::raise("value overflows numeric format"),
        Value::Numeric,
    )
}

/// `numeric` division with PostgreSQL's result scale (`select_div_scale`):
/// enough fractional digits for 16 significant ones, and at least either
/// operand's scale.
pub(crate) fn numeric_div(a: rust_decimal::Decimal, b: rust_decimal::Decimal) -> Value {
    if b.is_zero() {
        return crate::eval_error::raise("division by zero");
    }
    // The weight (position of the first base-10000 digit) and that digit.
    fn weight_and_first(d: rust_decimal::Decimal) -> (i32, u32) {
        if d.is_zero() {
            return (0, 0);
        }
        let text = d.abs().normalize().to_string();
        let (int_part, frac_part) = text.split_once('.').unwrap_or((&text, ""));
        if int_part != "0" {
            let n = int_part.len();
            let first_len = (n - 1) % 4 + 1;
            (
                (n as i32 - 1) / 4,
                int_part[..first_len].parse().unwrap_or(0),
            )
        } else {
            let zeros = frac_part.len() - frac_part.trim_start_matches('0').len();
            let group = zeros / 4;
            let padded = format!("{frac_part:0<width$}", width = (group + 1) * 4);
            (
                -(group as i32) - 1,
                padded[group * 4..group * 4 + 4].parse().unwrap_or(0),
            )
        }
    }
    let (w1, f1) = weight_and_first(a);
    let (w2, f2) = weight_and_first(b);
    let qweight = w1 - w2 - i32::from(f1 <= f2);
    let rscale = (16 - qweight * 4)
        .max(a.scale() as i32)
        .max(b.scale() as i32)
        .clamp(0, 28) as u32;
    match a.checked_div(b) {
        Some(q) => {
            let mut q = q.round_dp_with_strategy(
                rscale,
                rust_decimal::RoundingStrategy::MidpointAwayFromZero,
            );
            q.rescale(rscale);
            Value::Numeric(q)
        }
        None => crate::eval_error::raise("value overflows numeric format"),
    }
}

/// Applies a unary operator to a value; type-invalid combinations yield `Null`.
pub(crate) fn apply_unary_op(op: ScalarUnaryOp, v: Value) -> Value {
    match (op, v) {
        (ScalarUnaryOp::Neg, Value::Int(i)) => Value::Int(-i),
        (ScalarUnaryOp::Neg, Value::Float(f)) => Value::Float(-f),
        (ScalarUnaryOp::Neg, Value::Numeric(d)) => Value::Numeric(-d),
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
            Value::Numeric(d) => Some(crate::value::decimal_to_f64(d)),
            _ => None,
        }
    };
    match op {
        Op::Add | Op::Sub | Op::Mul | Op::Div | Op::Mod => {
            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                return Value::Null;
            }
            // `jsonb - key`, `jsonb - index`, `jsonb - keys`.
            if let (Op::Sub, Value::Jsonb(json)) = (op, &l) {
                return crate::json_text::jsonb_delete(json.clone(), &r)
                    .map(Value::Jsonb)
                    .unwrap_or_else(crate::eval_error::raise);
            }
            // Exact decimal arithmetic when a numeric meets an integer or a
            // numeric (a float operand makes the result a float).
            if matches!(l, Value::Numeric(_)) || matches!(r, Value::Numeric(_)) {
                let decimal = |v: &Value| match v {
                    Value::Numeric(d) => Some(*d),
                    Value::Int(i) => Some(rust_decimal::Decimal::from(*i)),
                    _ => None,
                };
                if let (Some(a), Some(b)) = (decimal(&l), decimal(&r)) {
                    return numeric_arith(op, a, b);
                }
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
            } else if let Some((a, b)) = numeric_text_operands(&l, &r) {
                // A number's text, as an untyped literal is read.
                apply_binary_op(op, a, b)
            } else {
                // Date/time arithmetic, by the operands' text.
                let symbol = match op {
                    Op::Add => "+",
                    Op::Sub => "-",
                    Op::Mul => "*",
                    Op::Div => "/",
                    _ => "%",
                };
                crate::datetime::arith(symbol, &l, None, &r, None)
                    .unwrap_or_else(crate::eval_error::raise)
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
            // `jsonb || jsonb`, where an untyped literal side reads as jsonb.
            let as_jsonb = |v: &Value| match v {
                Value::Jsonb(j) => Some(j.clone()),
                Value::Text(t) => crate::json_text::parse(t).ok(),
                _ => None,
            };
            if matches!(l, Value::Jsonb(_)) || matches!(r, Value::Jsonb(_)) {
                if let (Some(a), Some(b)) = (as_jsonb(&l), as_jsonb(&r)) {
                    return Value::Jsonb(crate::json_text::jsonb_concat(a, b));
                }
            }
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

/// A number and a number's text as two numbers (`'5' + 1`).
fn numeric_text_operands(l: &Value, r: &Value) -> Option<(Value, Value)> {
    let numeric = |v: &Value| matches!(v, Value::Int(_) | Value::Float(_) | Value::Numeric(_));
    let parse = |text: &str| -> Option<Value> {
        let t = text.trim();
        t.parse::<i64>()
            .map(Value::Int)
            .ok()
            .or_else(|| crate::value::parse_decimal(t).map(Value::Numeric))
    };
    match (l, r) {
        (Value::Text(t), n) if numeric(n) => Some((parse(t)?, n.clone())),
        (n, Value::Text(t)) if numeric(n) => Some((n.clone(), parse(t)?)),
        _ => None,
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
        Some(j) if as_text => Value::Text(crate::json_text::jsonb_text(&j)),
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
        // `json` gives its members as written.
        Op::JsonGet | Op::JsonGetText | Op::JsonPath | Op::JsonPathText
            if let Value::Json(text) = l =>
        {
            let member = match op {
                Op::JsonGet | Op::JsonGetText => crate::json_text::json_member(text, r),
                _ => {
                    let mut cur = Some(text.as_str());
                    for key in texts(r) {
                        cur = cur.and_then(|doc| {
                            // A path step into an array is an index.
                            let step = match key.trim().parse::<i64>() {
                                Ok(i) if doc.trim_start().starts_with('[') => Value::Int(i),
                                _ => Value::Text(key.clone()),
                            };
                            crate::json_text::json_member(doc, &step)
                        });
                    }
                    cur
                }
            };
            match member {
                None => Value::Null,
                Some(m) if matches!(op, Op::JsonGetText | Op::JsonPathText) => {
                    crate::json_text::json_member_text(m)
                }
                Some(m) => Value::Json(m.to_string()),
            }
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
