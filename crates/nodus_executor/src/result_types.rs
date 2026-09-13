//! Result types determined from expressions and declared columns, independent
//! of result rows. Describe (LIMIT 0) and Execute must publish the same types.

use crate::{AggregateOp, ProjectionItem, ScalarExpr, Value};

fn aggregate_type(op: &AggregateOp, input: Option<String>) -> Option<String> {
    match op {
        AggregateOp::Count => Some("BIGINT".into()),
        AggregateOp::Min | AggregateOp::Max => input,
        AggregateOp::Sum => input.map(|ty| match ty.to_ascii_uppercase().as_str() {
            "INT" | "INTEGER" | "INT4" | "SMALLINT" | "INT2" => "BIGINT".into(),
            "BIGINT" | "INT8" => "NUMERIC".into(),
            _ => ty,
        }),
        AggregateOp::Avg => input.map(|ty| match ty.to_ascii_uppercase().as_str() {
            "REAL" | "FLOAT4" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => {
                "DOUBLE PRECISION".into()
            }
            _ => "NUMERIC".into(),
        }),
    }
}

fn literal_type(value: &Value) -> Option<String> {
    Some(
        match value {
            Value::Int(_) => "INTEGER",
            Value::Float(_) => "DOUBLE PRECISION",
            Value::Bool(_) => "BOOLEAN",
            Value::Text(_) => "TEXT",
            Value::Jsonb(_) => "JSONB",
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
