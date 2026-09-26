//! Result types determined from expressions and declared columns, independent
//! of result rows. Describe (LIMIT 0) and Execute must publish the same types.

use crate::{AggregateOp, ProjectionItem, ScalarBinaryOp, ScalarExpr, ScalarUnaryOp, Value};

fn aggregate_type(op: &AggregateOp, input: Option<String>) -> Option<String> {
    match op {
        AggregateOp::Count => Some("BIGINT".into()),
        AggregateOp::Min | AggregateOp::Max => input,
        AggregateOp::Sum => input.map(|ty| match ty.to_ascii_uppercase().as_str() {
            "INT" | "INTEGER" | "INT4" | "SMALLINT" | "INT2" => "BIGINT".into(),
            "BIGINT" | "INT8" => "NUMERIC".into(),
            _ => ty,
        }),
        AggregateOp::Avg
        | AggregateOp::StddevSamp
        | AggregateOp::StddevPop
        | AggregateOp::VarSamp
        | AggregateOp::VarPop => input.map(|ty| match ty.to_ascii_uppercase().as_str() {
            "REAL" | "FLOAT4" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => {
                "DOUBLE PRECISION".into()
            }
            _ => "NUMERIC".into(),
        }),
        AggregateOp::StringAgg => Some("TEXT".into()),
        AggregateOp::ArrayAgg => input.map(|ty| format!("{ty}[]")),
        AggregateOp::BoolAnd | AggregateOp::BoolOr => Some("BOOLEAN".into()),
        AggregateOp::JsonAgg | AggregateOp::JsonObjectAgg => Some("JSON".into()),
        AggregateOp::JsonbAgg | AggregateOp::JsonbObjectAgg => Some("JSONB".into()),
        AggregateOp::BitAnd | AggregateOp::BitOr => input,
    }
}

fn literal_type(value: &Value) -> Option<String> {
    Some(
        match value {
            Value::Int(i) if i32::try_from(*i).is_ok() => "INTEGER",
            Value::Int(_) => "BIGINT",
            Value::Float(_) => "DOUBLE PRECISION",
            Value::Numeric(_) => "NUMERIC",
            Value::Bool(_) => "BOOLEAN",
            Value::Text(_) => "TEXT",
            Value::Jsonb(_) => "JSONB",
            Value::Array(items) => {
                return items
                    .iter()
                    .find_map(literal_type)
                    .map(|element| format!("{element}[]"));
            }
            _ => return None,
        }
        .into(),
    )
}

fn scalar_type(expr: &ScalarExpr, column: &impl Fn(&str) -> Option<String>) -> Option<String> {
    match expr {
        ScalarExpr::Column(name) => column(name),
        ScalarExpr::Literal(value) => literal_type(value),
        ScalarExpr::Cast { target, .. } => Some(target.clone()),
        ScalarExpr::Binary { op, left, right } => {
            binary_type(*op, scalar_type(left, column), scalar_type(right, column))
        }
        ScalarExpr::Unary {
            op: ScalarUnaryOp::Neg,
            expr,
        } => scalar_type(expr, column),
        ScalarExpr::Unary {
            op: ScalarUnaryOp::Not,
            ..
        }
        | ScalarExpr::IsNull { .. }
        | ScalarExpr::IsBool { .. }
        | ScalarExpr::IsDistinctFrom { .. }
        | ScalarExpr::PatternMatch { .. }
        | ScalarExpr::InList { .. }
        | ScalarExpr::Quantified { .. } => Some("BOOLEAN".into()),
        ScalarExpr::Case {
            branches,
            else_result,
            ..
        } => branches
            .iter()
            .map(|(_, result)| result)
            .chain(else_result.as_deref())
            .find_map(|result| scalar_type(result, column)),
        ScalarExpr::Extract { .. } => Some("NUMERIC".into()),
        ScalarExpr::Function { name, args } => crate::functions::return_type(
            name,
            &args
                .iter()
                .map(|arg| scalar_type(arg, column))
                .collect::<Vec<_>>(),
        ),
        ScalarExpr::Aggregate {
            op, arg, arg_expr, ..
        } => aggregate_type(
            op,
            arg_expr
                .as_ref()
                .and_then(|expr| scalar_type(expr, column))
                .or_else(|| column(arg)),
        ),
        _ => None,
    }
}

/// Integer types by width, for arithmetic result types.
fn integer_rank(ty: &str) -> Option<u8> {
    match ty.to_ascii_uppercase().as_str() {
        "SMALLINT" | "INT2" => Some(1),
        "INT" | "INTEGER" | "INT4" | "SERIAL" => Some(2),
        "BIGINT" | "INT8" | "BIGSERIAL" => Some(3),
        _ => None,
    }
}

fn is_float(ty: &str) -> bool {
    matches!(
        ty.to_ascii_uppercase().as_str(),
        "REAL" | "FLOAT4" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" | "FLOAT"
    )
}

fn is_numeric(ty: &str) -> bool {
    let upper = ty.to_ascii_uppercase();
    upper.starts_with("NUMERIC") || upper.starts_with("DECIMAL")
}

/// The result type of a binary operator, where the operand types decide it.
fn binary_type(op: ScalarBinaryOp, left: Option<String>, right: Option<String>) -> Option<String> {
    use ScalarBinaryOp as Op;
    match op {
        Op::Eq
        | Op::NotEq
        | Op::Lt
        | Op::LtEq
        | Op::Gt
        | Op::GtEq
        | Op::And
        | Op::Or
        | Op::JsonHasKey
        | Op::JsonHasAnyKey
        | Op::JsonHasAllKeys
        | Op::Contains
        | Op::ContainedBy
        | Op::Overlap => Some("BOOLEAN".into()),
        Op::JsonGetText | Op::JsonPathText => Some("TEXT".into()),
        Op::JsonGet | Op::JsonPath => left,
        Op::Concat => match (&left, &right) {
            (Some(l), _) if l.ends_with("[]") => left,
            (_, Some(r)) if r.ends_with("[]") => right,
            _ => Some("TEXT".into()),
        },
        Op::Add | Op::Sub | Op::Mul | Op::Div | Op::Mod => {
            let (left, right) = (left?, right?);
            if let (Some(l), Some(r)) = (integer_rank(&left), integer_rank(&right)) {
                // Integer arithmetic is in the wider operand's type.
                Some(
                    match l.max(r) {
                        1 => "SMALLINT",
                        2 => "INTEGER",
                        _ => "BIGINT",
                    }
                    .into(),
                )
            } else if is_float(&left) || is_float(&right) {
                Some("DOUBLE PRECISION".into())
            } else if (is_numeric(&left) || integer_rank(&left).is_some())
                && (is_numeric(&right) || integer_rank(&right).is_some())
            {
                Some("NUMERIC".into())
            } else {
                None
            }
        }
    }
}

/// The type of a window function's result.
fn window_type(
    func_name: &str,
    args: &[String],
    column: &impl Fn(&str) -> Option<String>,
) -> Option<String> {
    let input = || args.first().and_then(|arg| column(arg));
    match func_name.to_ascii_uppercase().as_str() {
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" | "COUNT" => Some("BIGINT".into()),
        "PERCENT_RANK" | "CUME_DIST" => Some("DOUBLE PRECISION".into()),
        "SUM" => aggregate_type(&AggregateOp::Sum, input()),
        "AVG" => aggregate_type(&AggregateOp::Avg, input()),
        "MIN" | "MAX" | "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => input(),
        _ => None,
    }
}

/// `expr` with each integer arithmetic whose result is `integer` or
/// `smallint` checked against that type's range, as PostgreSQL computes it
/// in that type (`2147483647 + 1` fails rather than widening). `column`
/// gives the declared types of the columns it names.
pub(crate) fn check_integer_ranges(
    expr: &ScalarExpr,
    column: &impl Fn(&str) -> Option<String>,
) -> ScalarExpr {
    let checked = expr.map_children(&mut |e| check_integer_ranges(e, column));
    // `pg_typeof` reports the argument's declared type, which a value alone
    // cannot tell (a `smallint` column holds integers too); an untyped
    // string literal is `unknown`.
    if let ScalarExpr::Function { name, args } = &checked
        && name == "PG_TYPEOF"
        && let [arg] = args.as_slice()
    {
        if matches!(arg, ScalarExpr::Literal(Value::Text(_))) {
            return ScalarExpr::Literal(Value::Text("unknown".to_string()));
        }
        if let Some(ty) = scalar_type(arg, column) {
            let name = crate::functions::format_type_name(crate::MemExecutor::pg_type_oid(&ty));
            if name != "???" {
                return ScalarExpr::Literal(Value::Text(name));
            }
        }
    }
    let arithmetic = matches!(
        expr,
        ScalarExpr::Binary {
            op: ScalarBinaryOp::Add
                | ScalarBinaryOp::Sub
                | ScalarBinaryOp::Mul
                | ScalarBinaryOp::Div
                | ScalarBinaryOp::Mod,
            ..
        } | ScalarExpr::Unary {
            op: ScalarUnaryOp::Neg,
            ..
        }
    );
    match scalar_type(&checked, column).as_deref() {
        Some(ty @ ("INTEGER" | "SMALLINT")) if arithmetic => ScalarExpr::Function {
            name: INTEGER_RANGE.to_string(),
            args: vec![checked, ScalarExpr::Literal(Value::Text(ty.to_string()))],
        },
        _ => checked,
    }
}

/// The function [`check_integer_ranges`] wraps a result in: its first
/// argument, or an error when that is outside the type named by the second.
pub(crate) const INTEGER_RANGE: &str = "__INTEGER_RANGE__";

/// A condition with [`check_integer_ranges`] applied to its expressions.
pub(crate) fn check_filter_integer_ranges(
    filter: &crate::FilterExpr,
    column: &impl Fn(&str) -> Option<String>,
) -> crate::FilterExpr {
    use crate::FilterExpr as F;
    let check = |e: &ScalarExpr| check_integer_ranges(e, column);
    let recur = |f: &F| Box::new(check_filter_integer_ranges(f, column));
    match filter {
        F::And(a, b) => F::And(recur(a), recur(b)),
        F::Or(a, b) => F::Or(recur(a), recur(b)),
        F::Not(a) => F::Not(recur(a)),
        F::ExprCmp { left, op, right } => F::ExprCmp {
            left: check(left),
            op: op.clone(),
            right: check(right),
        },
        F::Scalar(e) => F::Scalar(check(e)),
        F::QuantifiedSubquery {
            left,
            op,
            subquery,
            all,
        } => F::QuantifiedSubquery {
            left: check(left),
            op: *op,
            subquery: subquery.clone(),
            all: *all,
        },
        other => other.clone(),
    }
}

/// The type of a scalar expression, given its columns' declared types.
pub(crate) fn expr_type(
    expr: &ScalarExpr,
    column: &impl Fn(&str) -> Option<String>,
) -> Option<String> {
    scalar_type(expr, column)
}

/// The type a value has, as a literal of it would.
pub(crate) fn value_type(value: &Value) -> String {
    literal_type(value).unwrap_or_else(|| "TEXT".to_string())
}

/// The type of a scalar expression that references no columns.
pub(crate) fn constant_expr_type(expr: &ScalarExpr) -> Option<String> {
    scalar_type(expr, &|_: &str| None)
}

pub(crate) fn projection_type(
    item: &ProjectionItem,
    column: impl Fn(&str) -> Option<String>,
) -> Option<String> {
    match item {
        ProjectionItem::Aggregate(op, arg) => aggregate_type(op, column(arg)),
        ProjectionItem::Expr { expr, .. } => scalar_type(expr, &column),
        ProjectionItem::Literal(value) | ProjectionItem::AliasedLiteral(value, _) => {
            literal_type(value)
        }
        ProjectionItem::Column(name) | ProjectionItem::AliasedColumn(name, _) => column(name),
        ProjectionItem::WindowFunction {
            func_name, args, ..
        } => window_type(func_name, args, &column),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_types_do_not_depend_on_rows() {
        assert_eq!(
            aggregate_type(&AggregateOp::Count, None).as_deref(),
            Some("BIGINT")
        );
        assert_eq!(
            aggregate_type(&AggregateOp::Sum, Some("INTEGER".into())).as_deref(),
            Some("BIGINT")
        );
        assert_eq!(
            aggregate_type(&AggregateOp::Sum, Some("BIGINT".into())).as_deref(),
            Some("NUMERIC")
        );
        assert_eq!(
            aggregate_type(&AggregateOp::Min, Some("DATE".into())).as_deref(),
            Some("DATE")
        );
    }
}
