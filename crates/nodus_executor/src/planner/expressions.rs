//! Expression-level parsing: SQL value/operand extraction and column-name resolution.
use super::*;
use crate::*;
use anyhow::Result;
use nodus_catalog::TableConstraint;

pub fn expr_to_value(expr: &sqlparser::ast::Expr, params: &[crate::Value]) -> Option<crate::Value> {
    use sqlparser::ast::{Expr, Value as SqlValue};
    match expr {
        Expr::Value(SqlValue::SingleQuotedString(s)) => Some(crate::Value::Text(s.clone())),
        Expr::Value(SqlValue::Number(n, _)) => {
            if let Ok(i) = n.parse::<i64>() {
                Some(crate::Value::Int(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Some(crate::Value::Float(f))
            } else {
                Some(crate::Value::Text(n.clone()))
            }
        }
        Expr::Value(SqlValue::Boolean(b)) => Some(crate::Value::Bool(*b)),
        Expr::Value(SqlValue::Null) => Some(crate::Value::Null),
        Expr::Value(SqlValue::Placeholder(s)) => {
            if let Some(stripped) = s.strip_prefix('$') {
                if let Ok(idx) = stripped.parse::<usize>() {
                    if idx > 0 && idx <= params.len() {
                        return Some(params[idx - 1].clone());
                    }
                }
            }
            None
        }
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
        Expr::JsonAccess {
            left,
            operator,
            right,
        } => {
            let left_col = extract_col_name(left)?;
            let right_val = match &**right {
                Expr::Value(v) => match v {
                    sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
                    sqlparser::ast::Value::Number(n, _) => n.clone(),
                    _ => return None,
                },
                _ => return None,
            };
            let op_str = match operator {
                sqlparser::ast::JsonOperator::LongArrow => "->>",
                sqlparser::ast::JsonOperator::Arrow => "->",
                sqlparser::ast::JsonOperator::HashArrow => "#>",
                sqlparser::ast::JsonOperator::HashLongArrow => "#>>",
                _ => return None,
            };
            Some(format!("{}{}'{}'", left_col, op_str, right_val))
        }
        Expr::Cast { expr, .. } => extract_col_name(expr),
        // Aggregate function calls render to a canonical `FUNC(arg)` key so a
        // `HAVING` predicate can name them. Non-aggregate functions stay `None`
        // so they don't silently match in a `WHERE` clause.
        Expr::Function(func) => {
            use sqlparser::ast::{FunctionArg, FunctionArgExpr};
            let fname = func.name.to_string().to_uppercase();
            if !matches!(fname.as_str(), "COUNT" | "SUM" | "MIN" | "MAX" | "AVG") {
                return None;
            }
            let arg = match func.args.first() {
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
        results,
        else_result: Some(else_result),
    } = expr
    else {
        return None;
    };
    let condition = conditions.first()?;
    let then_expr = results.first()?;
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
        results,
        else_result,
    } = expr
    else {
        return None;
    };
    let mut branches = Vec::new();
    for (cond, res) in conditions.iter().zip(results.iter()) {
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

/// Extracts a window/scalar function's arguments as strings: column names via
/// `extract_col_name`, or numeric/string literals (e.g. the LAG/LEAD offset).
pub(crate) fn window_args(func: &sqlparser::ast::Function) -> Vec<String> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Value as SqlValue};
    func.args
        .iter()
        .filter_map(|a| match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                extract_col_name(e).or_else(|| match e {
                    Expr::Value(SqlValue::Number(n, _)) => Some(n.clone()),
                    Expr::Value(SqlValue::SingleQuotedString(s)) => Some(s.clone()),
                    _ => None,
                })
            }
            _ => None,
        })
        .collect()
}
