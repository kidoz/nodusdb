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
        Expr::Value(_) | Expr::Array(_) => expr_to_value(expr, params),
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
        // SUBSTRING/TRIM parse to dedicated nodes; reuse the lowering + evaluator
        // (no columns are referenced in a FROM-less SELECT).
        Expr::Substring { .. } | Expr::Trim { .. } => {
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
        ColumnType::Int => match &v {
            Value::Int(_) => v,
            Value::Float(f) => Value::Int(f.round() as i64),
            Value::Bool(b) => Value::Int(i64::from(*b)),
            Value::Text(s) => s
                .trim()
                .parse::<i64>()
                .map(Value::Int)
                .or_else(|_| s.trim().parse::<f64>().map(|f| Value::Int(f.round() as i64)))
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
        ColumnType::Text => Value::Text(render(&v)),
    }
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
        Expr::Value(_) | Expr::Array(_) => expr_to_value(expr, params).map(ScalarExpr::Literal),
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
        Expr::Function(func) => {
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
        _ => None,
    }
}

/// Evaluates a [`ScalarExpr`] against one row (column values in `col_names`
/// order). Unresolvable columns and type-invalid operations yield `Null`.
pub(crate) fn eval_scalar_expr(expr: &ScalarExpr, row: &[Value], col_names: &[String]) -> Value {
    match expr {
        ScalarExpr::Literal(v) => v.clone(),
        ScalarExpr::Column(name) => col_names
            .iter()
            .position(|c| c == name || c.ends_with(&format!(".{name}")))
            .and_then(|i| row.get(i))
            .cloned()
            .unwrap_or(Value::Null),
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
            } else {
                let (Some(a), Some(b)) = (as_f64(&l), as_f64(&r)) else {
                    return Value::Null;
                };
                match op {
                    Op::Add => Value::Float(a + b),
                    Op::Sub => Value::Float(a - b),
                    Op::Mul => Value::Float(a * b),
                    Op::Div if b != 0.0 => Value::Float(a / b),
                    Op::Mod if b != 0.0 => Value::Float(a % b),
                    _ => Value::Null,
                }
            }
        }
        Op::Eq | Op::NotEq | Op::Lt | Op::LtEq | Op::Gt | Op::GtEq => {
            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                return Value::Null;
            }
            use std::cmp::Ordering::{Greater, Less};
            let ord = compare(&l, &r);
            Value::Bool(match op {
                Op::Eq => values_equal(&l, &r),
                Op::NotEq => !values_equal(&l, &r),
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
