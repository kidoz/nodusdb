//! Wire-protocol shaping: execution-error → SQLSTATE mapping, `ErrorResponse`
//! construction, `RowDescription` building, COPY-statement detection, and
//! per-column result-format selection.

use std::sync::Arc;

use pgwire::api::Type;
use pgwire::api::results::{FieldFormat, FieldInfo};
use pgwire::error::{ErrorInfo, PgWireError};
use pgwire::messages::data::{FieldDescription, RowDescription};

use crate::POSTGRES_TYPEMOD_NONE;
use crate::type_map::map_declared_type;

/// Maps an executor error message to the closest PostgreSQL SQLSTATE so drivers
/// and ORMs can branch on the error class instead of parsing English text.
///
/// The match is intentionally message-driven: the executor raises plain
/// `anyhow` errors, and this is the single place that classifies them. Order
/// matters — more specific substrings are tested before generic ones (e.g. a
/// missing *column* before a missing *relation*). Anything unrecognized falls
/// back to `XX000` (`internal_error`), which is the signal that a new class
/// should be added here rather than silently mislabeled.
pub(crate) fn sqlstate_for_execution_error(err_str: &str) -> &'static str {
    // Integrity-constraint violations (class 23).
    if err_str.contains("Unique constraint violation") {
        "23505" // unique_violation
    } else if err_str.contains("cannot be NULL") {
        "23502" // not_null_violation
    } else if err_str.contains("violates foreign key constraint") {
        "23503" // foreign_key_violation
    } else if err_str.contains("violates check constraint") {
        "23514" // check_violation
    // Invalid text representation for a typed value (class 22).
    } else if err_str.contains("invalid input syntax") {
        "22P02" // invalid_text_representation
    // Duplicate object on CREATE without IF NOT EXISTS (class 42). Catalog and
    // DDL paths phrase this differently ("Database X already exists" vs
    // "relation \"x\" already exists"), so classify case-insensitively.
    } else if err_str.contains("already exists") {
        let lower = err_str.to_ascii_lowercase();
        if lower.contains("database") {
            "42P04" // duplicate_database
        } else if lower.contains("schema") {
            "42P06" // duplicate_schema
        } else {
            "42P07" // duplicate_table (relation / table / index / view)
        }
    // Missing object (class 42 / 3B).
    } else if err_str.contains("savepoint \"") && err_str.contains("does not exist") {
        "3B001" // invalid_savepoint_specification
    } else if err_str.contains("column") && err_str.contains("does not exist") {
        "42703" // undefined_column
    } else if err_str.contains("does not exist") {
        "42P01" // undefined_table (relation / index "x" does not exist)
    } else {
        "XX000" // internal_error
    }
}

pub(crate) fn user_error(severity: &str, code: &str, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        severity.to_owned(),
        code.to_owned(),
        message.into(),
    )))
}

pub(crate) fn row_description(fields: &[FieldInfo]) -> RowDescription {
    RowDescription::new(
        fields
            .iter()
            .map(|field| {
                let ty = field.datatype();
                FieldDescription::new(
                    field.name().to_owned(),
                    field.table_id().unwrap_or(0),
                    field.column_id().unwrap_or(0),
                    ty.oid(),
                    type_size(ty),
                    POSTGRES_TYPEMOD_NONE,
                    field.format().value(),
                )
            })
            .collect(),
    )
}

pub(crate) fn row_description_from_metadata(
    columns: &[(String, String)],
    format_for: impl Fn(usize, &Type) -> FieldFormat,
) -> RowDescription {
    RowDescription::new(
        columns
            .iter()
            .enumerate()
            .map(|(i, (name, declared))| {
                let declared = map_declared_type(declared);
                let format = effective_result_format(&declared.ty, format_for(i, &declared.ty));
                FieldDescription::new(
                    name.clone(),
                    0,
                    0,
                    declared.ty.oid(),
                    type_size(&declared.ty),
                    declared.typmod,
                    format.value(),
                )
            })
            .collect(),
    )
}

pub(crate) fn is_copy_from_stdin(query: &str) -> bool {
    let q = query.trim().to_ascii_uppercase();
    q.starts_with("COPY ") && q.contains(" FROM STDIN")
}

pub(crate) fn is_copy_to_stdout(query: &str) -> bool {
    let q = query.trim().to_ascii_uppercase();
    q.starts_with("COPY ") && q.contains(" TO STDOUT")
}

pub(crate) fn copy_format_code(query: &str) -> i8 {
    let q = query.trim().to_ascii_uppercase();
    if q.contains("FORMAT BINARY") || q.contains("WITH BINARY") {
        1
    } else {
        0
    }
}

pub(crate) fn copy_column_count(query: &str) -> i16 {
    let upper = query.to_ascii_uppercase();
    let boundary = upper
        .find(" FROM STDIN")
        .or_else(|| upper.find(" TO STDOUT"))
        .unwrap_or(query.len());
    let head = &query[..boundary];
    let Some(open) = head.find('(') else {
        return 0;
    };
    let Some(close) = head.rfind(')') else {
        return 0;
    };
    if close <= open {
        return 0;
    }
    head[open + 1..close]
        .split(',')
        .filter(|part| !part.trim().is_empty())
        .count()
        .try_into()
        .unwrap_or(i16::MAX)
}

pub(crate) fn type_size(ty: &Type) -> i16 {
    match *ty {
        Type::BOOL => 1,
        Type::CHAR => 1,
        Type::INT2 => 2,
        Type::INT4
        | Type::OID
        | Type::REGCLASS
        | Type::REGCONFIG
        | Type::REGDICTIONARY
        | Type::REGNAMESPACE
        | Type::REGOPER
        | Type::REGOPERATOR
        | Type::REGPROC
        | Type::REGPROCEDURE
        | Type::REGROLE
        | Type::REGTYPE
        | Type::FLOAT4
        | Type::DATE => 4,
        Type::INT8 | Type::FLOAT8 | Type::TIME | Type::TIMESTAMP | Type::TIMESTAMPTZ => 8,
        Type::UUID => 16,
        Type::NAME => 64,
        _ => -1,
    }
}

pub(crate) fn supports_binary_result(ty: &Type) -> bool {
    !matches!(
        *ty,
        Type::CHAR
            | Type::CHAR_ARRAY
            | Type::NUMERIC
            | Type::NUMERIC_ARRAY
            | Type::REGCLASS
            | Type::REGCONFIG
            | Type::REGDICTIONARY
            | Type::REGNAMESPACE
            | Type::REGOPER
            | Type::REGOPERATOR
            | Type::REGPROC
            | Type::REGPROCEDURE
            | Type::REGROLE
            | Type::REGTYPE
            | Type::REGTYPE_ARRAY
            | Type::TIMETZ
    )
}

pub(crate) fn effective_result_format(ty: &Type, requested: FieldFormat) -> FieldFormat {
    if requested == FieldFormat::Binary && supports_binary_result(ty) {
        FieldFormat::Binary
    } else {
        FieldFormat::Text
    }
}

pub(crate) fn field_info_for_output(
    names: &[String],
    declared_types: &[String],
    requested_format: impl Fn(usize, &Type) -> FieldFormat,
) -> Arc<Vec<FieldInfo>> {
    Arc::new(
        names
            .iter()
            .zip(declared_types.iter())
            .enumerate()
            .map(|(i, (name, declared))| {
                let declared = map_declared_type(declared);
                let _typmod = declared.typmod;
                let format =
                    effective_result_format(&declared.ty, requested_format(i, &declared.ty));
                FieldInfo::new(name.clone(), None, None, declared.ty, format)
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::sqlstate_for_execution_error;

    #[test]
    fn maps_constraint_violations() {
        assert_eq!(
            sqlstate_for_execution_error("Unique constraint violation on primary key"),
            "23505"
        );
        assert_eq!(
            sqlstate_for_execution_error("Column email cannot be NULL"),
            "23502"
        );
        assert_eq!(
            sqlstate_for_execution_error("violates foreign key constraint"),
            "23503"
        );
        assert_eq!(
            sqlstate_for_execution_error("violates check constraint"),
            "23514"
        );
    }

    #[test]
    fn maps_duplicate_objects_case_insensitively() {
        // DDL-path phrasing.
        assert_eq!(
            sqlstate_for_execution_error("relation \"users\" already exists"),
            "42P07"
        );
        // Catalog-path phrasing.
        assert_eq!(
            sqlstate_for_execution_error("Table users already exists"),
            "42P07"
        );
        assert_eq!(
            sqlstate_for_execution_error("Schema app already exists"),
            "42P06"
        );
        assert_eq!(
            sqlstate_for_execution_error("Database shop already exists"),
            "42P04"
        );
    }

    #[test]
    fn maps_missing_objects() {
        assert_eq!(
            sqlstate_for_execution_error("relation \"pg_catalog.nope\" does not exist"),
            "42P01"
        );
        assert_eq!(
            sqlstate_for_execution_error("index \"idx_x\" does not exist"),
            "42P01"
        );
        assert_eq!(
            sqlstate_for_execution_error("column \"missing\" does not exist"),
            "42703"
        );
        assert_eq!(
            sqlstate_for_execution_error("savepoint \"sp1\" does not exist"),
            "3B001"
        );
    }

    #[test]
    fn maps_invalid_text_representation() {
        assert_eq!(
            sqlstate_for_execution_error("invalid input syntax for type integer: \"abc\""),
            "22P02"
        );
    }

    #[test]
    fn falls_back_to_internal_error() {
        assert_eq!(
            sqlstate_for_execution_error("something nobody classified yet"),
            "XX000"
        );
    }
}
