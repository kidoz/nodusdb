//! WHERE/ON predicate parsing into FilterExpr.
use super::*;
use crate::*;
use anyhow::Result;
use nodus_catalog::TableConstraint;

pub(crate) fn compare_op(op: &sqlparser::ast::BinaryOperator) -> Option<CompareOp> {
    use sqlparser::ast::BinaryOperator::*;
    match op {
        Eq => Some(CompareOp::Eq),
        NotEq => Some(CompareOp::Ne),
        Lt => Some(CompareOp::Lt),
        LtEq => Some(CompareOp::Le),
        Gt => Some(CompareOp::Gt),
        GtEq => Some(CompareOp::Ge),
        AtArrow => Some(CompareOp::Contains),
        ArrowAt => Some(CompareOp::ContainedBy),
        Custom(s) if s == "@>" => Some(CompareOp::Contains),
        Custom(s) if s == "<@" => Some(CompareOp::ContainedBy),
        _ => None,
    }
}

/// Parses a `WHERE` clause into a conjunction of `column <op> literal`
/// predicates (AND only; other expressions are ignored).
pub(crate) fn parse_predicates(
    selection: &Option<sqlparser::ast::Expr>,
    params: &[Value],
) -> Option<FilterExpr> {
    if let Some(expr) = selection {
        parse_filter_expr(expr, params)
    } else {
        None
    }
}

pub(crate) fn parse_filter_expr(
    expr: &sqlparser::ast::Expr,
    params: &[Value],
) -> Option<FilterExpr> {
    use sqlparser::ast::{BinaryOperator, Expr};
    match expr {
        Expr::Nested(inner) => parse_filter_expr(inner, params),
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            let l = parse_filter_expr(left, params);
            let r = parse_filter_expr(right, params);
            match (l, r) {
                (Some(l), Some(r)) => Some(FilterExpr::And(Box::new(l), Box::new(r))),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None,
            }
        }
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Or => {
            let l = parse_filter_expr(left, params);
            let r = parse_filter_expr(right, params);
            match (l, r) {
                (Some(l), Some(r)) => Some(FilterExpr::Or(Box::new(l), Box::new(r))),
                _ => None,
            }
        }
        Expr::UnaryOp { op, expr } if *op == sqlparser::ast::UnaryOperator::Not => {
            if let Some(inner) = parse_filter_expr(expr, params) {
                Some(FilterExpr::Not(Box::new(inner)))
            } else {
                None
            }
        }
        Expr::IsNull(expr) => extract_col_name(expr).map(FilterExpr::IsNull),
        Expr::IsNotNull(expr) => extract_col_name(expr).map(FilterExpr::IsNotNull),
        Expr::Like {
            negated,
            expr,
            pattern,
            ..
        } => {
            let left_col = extract_col_name(expr)?;
            let right_op = extract_operand(pattern, params)?;
            Some(FilterExpr::Like {
                left: left_col,
                right: right_op,
                negated: *negated,
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let left_col = extract_col_name(expr)?;
            let mut ops = Vec::new();
            for item in list {
                if let Some(op) = extract_operand(item, params) {
                    ops.push(op);
                } else {
                    return None;
                }
            }
            Some(FilterExpr::InList {
                left: left_col,
                list: ops,
                negated: *negated,
            })
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let left_col = extract_col_name(expr)?;
            let sub_plan = plan_query(subquery, params).ok()?;
            Some(FilterExpr::InSubquery {
                left: left_col,
                subquery: Box::new(sub_plan),
                negated: *negated,
            })
        }
        Expr::Exists { subquery, negated } => {
            let sub_plan = plan_query(subquery, params).ok()?;
            Some(FilterExpr::Exists {
                subquery: Box::new(sub_plan),
                negated: *negated,
            })
        }
        Expr::BinaryOp { left, op, right } => {
            let cmp = compare_op(op)?;
            if let Some(left_col) = extract_col_name(left) {
                // `col <op> (scalar subquery)`.
                if let Expr::Subquery(query) = &**right {
                    let sub_plan = plan_query(query, params).ok()?;
                    return Some(FilterExpr::CompareSubquery {
                        left: left_col,
                        op: cmp,
                        subquery: Box::new(sub_plan),
                    });
                }
                if let Some(right_op) = extract_operand(right, params) {
                    return Some(FilterExpr::Predicate(Predicate {
                        left: left_col,
                        op: cmp,
                        right: right_op,
                    }));
                }
            }
            // A computed side (e.g. `n % 2 = 0`, `a = b + 1`): lower both to
            // scalar expressions and compare per row.
            let l = lower_scalar(left, params)?;
            let r = lower_scalar(right, params)?;
            Some(FilterExpr::ExprCmp {
                left: l,
                op: cmp,
                right: r,
            })
        }
        // `x BETWEEN a AND b` -> `x >= a AND x <= b`; NOT BETWEEN -> `x < a OR x > b`.
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let col = extract_col_name(expr)?;
            let low_op = extract_operand(low, params)?;
            let high_op = extract_operand(high, params)?;
            let lo = FilterExpr::Predicate(Predicate {
                left: col.clone(),
                op: if *negated { CompareOp::Lt } else { CompareOp::Ge },
                right: low_op,
            });
            let hi = FilterExpr::Predicate(Predicate {
                left: col,
                op: if *negated { CompareOp::Gt } else { CompareOp::Le },
                right: high_op,
            });
            if *negated {
                Some(FilterExpr::Or(Box::new(lo), Box::new(hi)))
            } else {
                Some(FilterExpr::And(Box::new(lo), Box::new(hi)))
            }
        }
        _ => None,
    }
}
