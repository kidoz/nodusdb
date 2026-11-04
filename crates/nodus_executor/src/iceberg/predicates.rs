//! Validate and bind the deliberately small external SQL filter subset.
use super::*;
use chrono::Datelike;
use nodus_common::iceberg::{Field, ScanLiteral, ScanOp, ScanPredicate};

pub(super) struct Bound {
    pub residual: Option<FilterExpr>,
    pub pushdown: Vec<ScanPredicate>,
    pub columns: Vec<usize>,
    pub dates: Vec<usize>,
}
pub(super) fn bind(
    filter: Option<&FilterExpr>,
    descriptor: &Descriptor,
    relation: &str,
    alias: Option<&str>,
) -> Result<Bound> {
    let mut bound = Bound {
        residual: None,
        pushdown: vec![],
        columns: vec![],
        dates: vec![],
    };
    if let Some(filter) = filter {
        bound.residual = Some(bind_expr(
            filter, descriptor, relation, alias, &mut bound, 0,
        )?);
    }
    Ok(bound)
}
fn bind_expr(
    expr: &FilterExpr,
    descriptor: &Descriptor,
    relation: &str,
    alias: Option<&str>,
    bound: &mut Bound,
    depth: usize,
) -> Result<FilterExpr> {
    if depth > 16 || bound.pushdown.len() >= 64 {
        return Err(unsupported(
            "Iceberg WHERE exceeds 64 terms or 16 nesting levels",
        ));
    }
    if let FilterExpr::And(left, right) = expr {
        return Ok(FilterExpr::And(
            Box::new(bind_expr(
                left,
                descriptor,
                relation,
                alias,
                bound,
                depth + 1,
            )?),
            Box::new(bind_expr(
                right,
                descriptor,
                relation,
                alias,
                bound,
                depth + 1,
            )?),
        ));
    }
    let (column, op, value) = match expr {
        FilterExpr::Predicate(Predicate {
            left,
            op,
            right: Operand::Literal(value),
        }) => {
            let op = match op {
                CompareOp::Eq => ScanOp::Eq,
                CompareOp::Ne => ScanOp::Ne,
                CompareOp::Lt => ScanOp::Lt,
                CompareOp::Le => ScanOp::Le,
                CompareOp::Gt => ScanOp::Gt,
                CompareOp::Ge => ScanOp::Ge,
                _ => return Err(subset_error()),
            };
            (left, op, Some(value))
        }
        FilterExpr::IsNull(column) => (column, ScanOp::IsNull, None),
        FilterExpr::IsNotNull(column) => (column, ScanOp::IsNotNull, None),
        _ => return Err(subset_error()),
    };
    let index = resolve(column, descriptor, relation, alias)?;
    let field = &descriptor.fields[index];
    if value.is_some() && field.sql_type == "DATE" && !bound.dates.contains(&index) {
        bound.dates.push(index);
    }

    let (value, literal) = match value {
        Some(value) => literal(value, field)?,
        None => (Value::Null, ScanLiteral::Null),
    };
    if !bound.columns.contains(&index) {
        bound.columns.push(index);
    }
    bound.pushdown.push(ScanPredicate {
        field_id: field.id,
        column: field.name.clone(),
        op,
        value: literal,
    });
    Ok(match op {
        ScanOp::IsNull => FilterExpr::IsNull(field.name.clone()),
        ScanOp::IsNotNull => FilterExpr::IsNotNull(field.name.clone()),
        _ => FilterExpr::Predicate(Predicate {
            left: field.name.clone(),
            op: match op {
                ScanOp::Eq => CompareOp::Eq,
                ScanOp::Ne => CompareOp::Ne,
                ScanOp::Lt => CompareOp::Lt,
                ScanOp::Le => CompareOp::Le,
                ScanOp::Gt => CompareOp::Gt,
                ScanOp::Ge => CompareOp::Ge,
                _ => return Err(subset_error()),
            },
            right: Operand::Literal(value),
        }),
    })
}
fn subset_error() -> anyhow::Error {
    unsupported(
        "Iceberg WHERE supports AND comparisons with integer, boolean, text or ISO date literals, and IS NULL/IS NOT NULL only",
    )
}
fn literal(value: &Value, field: &Field) -> Result<(Value, ScanLiteral)> {
    if matches!(value, Value::Text(text) if text.len() > 4096) {
        return Err(unsupported("Iceberg predicate literals exceed 4 KiB"));
    }
    if matches!(value, Value::Null) {
        return Ok((Value::Null, ScanLiteral::Null));
    }
    let literal = match (field.sql_type.as_str(), value) {
        ("INTEGER" | "BIGINT", Value::Int(value)) => ScanLiteral::Int(*value),
        ("BOOLEAN", Value::Bool(value)) => ScanLiteral::Bool(*value),
        ("TEXT", Value::Text(value)) => ScanLiteral::String(value.clone()),
        ("DATE", Value::Text(value)) => {
            let days = date_days(value)?;
            return Ok((Value::Int(i64::from(days)), ScanLiteral::Date(days)));
        }
        _ => return Err(subset_error()),
    };
    Ok((value.clone(), literal))
}
pub(super) fn resolve(
    column: &str,
    descriptor: &Descriptor,
    relation: &str,
    alias: Option<&str>,
) -> Result<usize> {
    let short = match column.rsplit_once('.') {
        Some((qualifier, short))
            if qualifier == alias.unwrap_or(relation) || qualifier == relation =>
        {
            short
        }
        Some(_) => {
            return Err(
                crate::error_fields::DbError::new("missing FROM-clause entry for table")
                    .code("42P01")
                    .into(),
            );
        }
        None => column,
    };
    descriptor
        .fields
        .iter()
        .position(|field| field.name == short)
        .ok_or_else(|| {
            crate::error_fields::DbError::new(format!("column \"{short}\" does not exist"))
                .code("42703")
                .into()
        })
}

// Compare date ordinals, not their display strings (notably BCE/large years).
pub(super) fn date_days(text: &str) -> Result<i32> {
    let date = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|_| {
        unsupported("Iceberg date predicates require valid ISO YYYY-MM-DD literals")
    })?;
    Ok(date.num_days_from_ce() - 719163)
}
