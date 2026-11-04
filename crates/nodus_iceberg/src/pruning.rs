//! Conservative file pruning. Exact SQL evaluation remains in the executor.
use crate::Error;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, PrimitiveType, Schema, Type};
use nodus_common::iceberg::{ScanLiteral, ScanOp, ScanPredicate};

/// Bind by field ID across metadata-only renames. A type promotion or a field
/// absent from the snapshot disables that atom's pruning, never its residual.
pub(crate) fn predicate(
    terms: &[ScanPredicate],
    current: &Schema,
    snapshot: &Schema,
) -> Result<Option<Predicate>, Error> {
    if terms.len() > 64 {
        return Err(Error::Unsupported("more than 64 scan predicates"));
    }
    let mut result: Option<Predicate> = None;
    for term in terms {
        let field = current
            .field_by_id(term.field_id)
            .ok_or(Error::Unsupported("predicate field is missing"))?;
        if field.name != term.column {
            return Err(Error::Unsupported("predicate field identity changed"));
        }
        validate(term, field.field_type.as_ref())?;
        let Some(old) = snapshot.field_by_id(term.field_id) else {
            continue;
        };
        if old.field_type != field.field_type
            || snapshot.field_by_name(&old.name).map(|field| field.id) != Some(term.field_id)
        {
            continue;
        }
        let reference = Reference::new(&old.name);
        let atom = match term.op {
            ScanOp::IsNull => reference.is_null(),
            ScanOp::IsNotNull => reference.is_not_null(),
            _ if matches!(term.value, ScanLiteral::Null) => Predicate::AlwaysFalse,
            _ => {
                let datum = match &term.value {
                    // Avoid depending on out-of-range narrowing in the binder.
                    ScanLiteral::Int(value)
                        if matches!(
                            field.field_type.as_ref(),
                            Type::Primitive(PrimitiveType::Int)
                        ) =>
                    {
                        let Ok(value) = i32::try_from(*value) else {
                            continue;
                        };
                        Datum::int(value)
                    }
                    ScanLiteral::Int(value) => Datum::long(*value),
                    ScanLiteral::Bool(value) => Datum::bool(*value),
                    ScanLiteral::String(value) => Datum::string(value),
                    ScanLiteral::Date(value) => Datum::date(*value),
                    ScanLiteral::Null => {
                        return Err(Error::Unsupported("invalid predicate literal"));
                    }
                };
                match term.op {
                    ScanOp::Eq => reference.equal_to(datum),
                    ScanOp::Ne => reference.not_equal_to(datum),
                    ScanOp::Lt => reference.less_than(datum),
                    ScanOp::Le => reference.less_than_or_equal_to(datum),
                    ScanOp::Gt => reference.greater_than(datum),
                    ScanOp::Ge => reference.greater_than_or_equal_to(datum),
                    _ => return Err(Error::Unsupported("invalid predicate operator")),
                }
            }
        };
        result = Some(result.map_or(atom.clone(), |previous| previous.and(atom)));
    }
    Ok(result)
}
fn validate(term: &ScanPredicate, ty: &Type) -> Result<(), Error> {
    if matches!(term.op, ScanOp::IsNull | ScanOp::IsNotNull)
        || matches!(term.value, ScanLiteral::Null)
    {
        return Ok(());
    }
    let supported = matches!(
        (ty, &term.value),
        (
            Type::Primitive(PrimitiveType::Int | PrimitiveType::Long),
            ScanLiteral::Int(_)
        ) | (
            Type::Primitive(PrimitiveType::Boolean),
            ScanLiteral::Bool(_)
        ) | (
            Type::Primitive(PrimitiveType::String),
            ScanLiteral::String(_)
        ) | (Type::Primitive(PrimitiveType::Date), ScanLiteral::Date(_))
    );
    if !supported {
        return Err(Error::Unsupported("predicate type or literal"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::NestedField;
    use std::sync::Arc;
    fn schema(name: &str, id: i32, ty: PrimitiveType) -> Schema {
        Schema::builder()
            .with_fields([Arc::new(NestedField::optional(
                id,
                name,
                Type::Primitive(ty),
            ))])
            .build()
            .unwrap()
    }
    fn term(name: &str, id: i32, value: ScanLiteral) -> ScanPredicate {
        ScanPredicate {
            field_id: id,
            column: name.into(),
            op: ScanOp::Eq,
            value,
        }
    }
    #[test]
    fn snapshot_binding_follows_ids_after_a_metadata_only_rename() {
        let current = schema("label", 1, PrimitiveType::String);
        let old = schema("name", 1, PrimitiveType::String);
        let predicate = predicate(
            &[term("label", 1, ScanLiteral::String("alpha".into()))],
            &current,
            &old,
        )
        .unwrap()
        .unwrap();
        assert_eq!(predicate.to_string(), "name = \"alpha\"");
    }
    #[test]
    fn additions_promotions_and_narrowing_disable_only_the_unsafe_pruning_atom() {
        let current = schema("id", 1, PrimitiveType::Long);
        let old = schema("id", 1, PrimitiveType::Int);
        assert!(
            predicate(&[term("id", 1, ScanLiteral::Int(30))], &current, &old)
                .unwrap()
                .is_none()
        );
        let missing = schema("other", 2, PrimitiveType::Long);
        assert!(
            predicate(&[term("id", 1, ScanLiteral::Int(30))], &current, &missing)
                .unwrap()
                .is_none()
        );
        assert!(
            predicate(&[term("id", 1, ScanLiteral::Int(i64::MAX))], &old, &old)
                .unwrap()
                .is_none()
        );
        let mut null = term("id", 1, ScanLiteral::Null);
        assert_eq!(
            predicate(&[null.clone()], &current, &current).unwrap(),
            Some(Predicate::AlwaysFalse)
        );
        null.column = "wrong".into();
        assert!(predicate(&[null], &current, &current).is_err());
    }
    #[test]
    fn incompatible_literal_types_are_rejected_instead_of_cast_or_ignored() {
        let schema = schema("id", 1, PrimitiveType::Long);
        assert!(
            predicate(
                &[term("id", 1, ScanLiteral::String("30".into()))],
                &schema,
                &schema
            )
            .is_err()
        );
    }
}
