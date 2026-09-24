//! WHERE/ON/HAVING predicate parsing into FilterExpr.
use super::*;
use crate::*;
use anyhow::Result;

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

/// Plans an optional `WHERE` clause. Every condition must be understood: an
/// unsupported one is an error rather than dropped, since a dropped condition
/// widens a query and makes UPDATE/DELETE touch every row.
pub(crate) fn parse_predicates(
    selection: &Option<sqlparser::ast::Expr>,
    params: &[Value],
) -> Result<Option<FilterExpr>> {
    selection
        .as_ref()
        .map(|expr| parse_filter_expr(expr, params))
        .transpose()
}

/// Plans a `HAVING` clause as one boolean expression over each group, so any
/// operator the scalar evaluator supports works on aggregates and group keys.
pub(crate) fn parse_having(expr: &sqlparser::ast::Expr, params: &[Value]) -> Result<FilterExpr> {
    lower_scalar(expr, params)
        .map(FilterExpr::Scalar)
        .ok_or_else(|| anyhow::anyhow!("Unsupported HAVING condition: {expr}"))
}

/// Plans a boolean condition. Column comparisons against literals keep their
/// typed fast paths (literals are coerced to the column type, and indexes can
/// use them); subqueries become subquery filters; anything else is evaluated as
/// a scalar expression. A condition none of these can express is an error.
pub(crate) fn parse_filter_expr(
    expr: &sqlparser::ast::Expr,
    params: &[Value],
) -> Result<FilterExpr> {
    use sqlparser::ast::{BinaryOperator, Expr};
    let planned = match expr {
        Expr::Nested(inner) => return parse_filter_expr(inner, params),
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            return Ok(FilterExpr::And(
                Box::new(parse_filter_expr(left, params)?),
                Box::new(parse_filter_expr(right, params)?),
            ));
        }
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Or => {
            return Ok(FilterExpr::Or(
                Box::new(parse_filter_expr(left, params)?),
                Box::new(parse_filter_expr(right, params)?),
            ));
        }
        Expr::UnaryOp { op, expr: inner } if *op == sqlparser::ast::UnaryOperator::Not => {
            return Ok(FilterExpr::Not(Box::new(parse_filter_expr(inner, params)?)));
        }
        Expr::IsNull(inner) => plain_column(inner).map(FilterExpr::IsNull),
        Expr::IsNotNull(inner) => plain_column(inner).map(FilterExpr::IsNotNull),
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => plain_column(inner).and_then(|left| {
            let list = list
                .iter()
                .map(|item| extract_operand(item, params))
                .collect::<Option<Vec<_>>>()?;
            Some(FilterExpr::InList {
                left,
                list,
                negated: *negated,
            })
        }),
        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => {
            let subquery = Box::new(plan_query(subquery, params)?);
            // Column left side, or a literal one (`1 IN (SELECT ...)`).
            match (extract_col_name(inner), expr_to_value(inner, params)) {
                (Some(left), _) => Some(FilterExpr::InSubquery {
                    left,
                    subquery,
                    negated: *negated,
                    left_value: None,
                }),
                (None, Some(value)) => Some(FilterExpr::InSubquery {
                    left: String::new(),
                    subquery,
                    negated: *negated,
                    left_value: Some(value),
                }),
                (None, None) => {
                    let left = lower_scalar(inner, params).ok_or_else(|| {
                        anyhow::anyhow!("Unsupported IN (subquery) operand: {inner}")
                    })?;
                    let quantified = FilterExpr::QuantifiedSubquery {
                        left,
                        op: ScalarBinaryOp::Eq,
                        subquery,
                        all: false,
                    };
                    Some(if *negated {
                        FilterExpr::Not(Box::new(quantified))
                    } else {
                        quantified
                    })
                }
            }
        }
        Expr::Exists { subquery, negated } => Some(FilterExpr::Exists {
            subquery: Box::new(plan_query(subquery, params)?),
            negated: *negated,
        }),
        // `x <op> ANY|ALL (SELECT ...)`.
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
        } if matches!(&**right, Expr::Subquery(_)) => {
            let Expr::Subquery(query) = &**right else {
                unreachable!("guarded by the match arm");
            };
            let op = compare_op_scalar(compare_op)
                .ok_or_else(|| anyhow::anyhow!("Unsupported quantified operator: {compare_op}"))?;
            let left = lower_scalar(left, params)
                .ok_or_else(|| anyhow::anyhow!("Unsupported quantified operand: {left}"))?;
            Some(FilterExpr::QuantifiedSubquery {
                left,
                op,
                subquery: Box::new(plan_query(query, params)?),
                all: matches!(expr, Expr::AllOp { .. }),
            })
        }
        Expr::BinaryOp { left, op, right } => match (compare_op(op), extract_col_name(left)) {
            (Some(cmp), Some(left_col)) => match &**right {
                // `col <op> (scalar subquery)`.
                Expr::Subquery(query) => Some(FilterExpr::CompareSubquery {
                    left: left_col,
                    op: cmp,
                    subquery: Box::new(plan_query(query, params)?),
                }),
                _ => extract_operand(right, params).map(|right| {
                    FilterExpr::Predicate(Predicate {
                        left: left_col,
                        op: cmp,
                        right,
                    })
                }),
            },
            _ => None,
        },
        // `x BETWEEN a AND b` -> `x >= a AND x <= b`; NOT BETWEEN -> `x < a OR x > b`.
        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => match (
            plain_column(inner),
            extract_operand(low, params),
            extract_operand(high, params),
        ) {
            (Some(col), Some(low), Some(high)) => {
                let (lo_op, hi_op) = if *negated {
                    (CompareOp::Lt, CompareOp::Gt)
                } else {
                    (CompareOp::Ge, CompareOp::Le)
                };
                let lo = Box::new(FilterExpr::Predicate(Predicate {
                    left: col.clone(),
                    op: lo_op,
                    right: low,
                }));
                let hi = Box::new(FilterExpr::Predicate(Predicate {
                    left: col,
                    op: hi_op,
                    right: high,
                }));
                Some(if *negated {
                    FilterExpr::Or(lo, hi)
                } else {
                    FilterExpr::And(lo, hi)
                })
            }
            _ => None,
        },
        _ => None,
    };
    if let Some(filter) = planned {
        return Ok(filter);
    }
    let condition = lower_scalar(expr, params).ok_or_else(|| {
        anyhow::anyhow!(
            unknown_function_error(expr)
                .unwrap_or_else(|| format!("Unsupported WHERE condition: {expr}"))
        )
    })?;
    if scalar_has_aggregate(&condition) {
        anyhow::bail!("aggregate functions are not allowed in WHERE");
    }
    Ok(FilterExpr::Scalar(condition))
}

/// A bare or qualified column reference (not a JSON access or cast).
fn plain_column(expr: &sqlparser::ast::Expr) -> Option<String> {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => extract_col_name(expr),
        _ => None,
    }
}

/// The comparison operators usable with `ANY`/`ALL`.
fn compare_op_scalar(op: &sqlparser::ast::BinaryOperator) -> Option<ScalarBinaryOp> {
    use sqlparser::ast::BinaryOperator as B;
    Some(match op {
        B::Eq => ScalarBinaryOp::Eq,
        B::NotEq => ScalarBinaryOp::NotEq,
        B::Lt => ScalarBinaryOp::Lt,
        B::LtEq => ScalarBinaryOp::LtEq,
        B::Gt => ScalarBinaryOp::Gt,
        B::GtEq => ScalarBinaryOp::GtEq,
        _ => return None,
    })
}
