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
    // Routing retries and unsupported consistency modes.
    if err_str.starts_with("shard unavailable:") || err_str.starts_with("shard routing changed") {
        "40001" // serialization_failure: retry against a current, available route
    } else if err_str.to_ascii_lowercase().contains("write conflict")
        || err_str.ends_with("; retry transaction")
    {
        // A lost write-write race (at write or commit time) or a stale participant
        // epoch rolled the transaction back; drivers and ORMs retry on this class.
        "40001" // serialization_failure
    } else if err_str.starts_with("unsupported linearizable cross-shard range read")
        || err_str.starts_with("unsupported row scan spanning multiple tables")
    {
        "0A000" // feature_not_supported
    // Statement shape errors raised while executing DML.
    } else if err_str.contains("more expressions than target columns")
        || err_str.contains("more target columns than expressions")
    {
        "42601" // syntax_error
    } else if err_str.contains("specified more than once") {
        "42701" // duplicate_column
    } else if err_str.contains("no unique or exclusion constraint matching the ON CONFLICT") {
        "42P10" // invalid_column_reference
    } else if err_str.contains("cannot affect row a second time") {
        "21000" // cardinality_violation
    // Integrity-constraint violations (class 23).
    } else if err_str.contains("Unique constraint violation") {
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
    // Runtime errors from evaluating expressions (class 22 / 21).
    } else if err_str == "division by zero" {
        "22012" // division_by_zero
    } else if err_str.starts_with("date/time field value out of range")
        || err_str.starts_with("date field value out of range")
        || err_str == "timestamp out of range"
    {
        "22008" // datetime_field_overflow
    } else if err_str.ends_with("out of range")
        || err_str.starts_with("value out of range")
        || err_str == "numeric field overflow"
        || err_str == "value overflows numeric format"
    {
        "22003" // numeric_value_out_of_range
    } else if err_str.starts_with("cannot take logarithm") {
        "2201E" // invalid_argument_for_logarithm
    } else if err_str == "cannot take square root of a negative number"
        || err_str == "zero raised to a negative power is undefined"
    {
        "2201F" // invalid_argument_for_power_function
    } else if err_str.starts_with("invalid regular expression") {
        "2201B" // invalid_regular_expression
    } else if err_str == "negative substring length not allowed" {
        "22011" // substring_error
    } else if err_str == "null value not allowed for object key" {
        "22004" // null_value_not_allowed
    } else if err_str == "more than one row returned by a subquery used as an expression" {
        "21000" // cardinality_violation
    } else if err_str.starts_with("aggregate functions are not allowed") {
        "42803" // grouping_error
    // Clause shape errors in ORDER BY / GROUP BY / DISTINCT / LIMIT.
    } else if err_str.ends_with("is not in select list")
        || err_str.starts_with("for SELECT DISTINCT, ORDER BY expressions")
        || err_str.starts_with("SELECT DISTINCT ON expressions must match")
        || err_str.contains("must not contain variables")
    {
        "42P10" // invalid_column_reference
    } else if err_str.starts_with("cannot insert a non-DEFAULT value into column")
        || err_str.ends_with("can only be updated to DEFAULT")
    {
        "428C9" // generated_always
    } else if err_str.starts_with("nextval: reached") || err_str.starts_with("setval: value") {
        "2200H" // sequence_generator_limit_exceeded
    } else if err_str.starts_with("currval of sequence")
        || err_str == "lastval is not yet defined in this session"
    {
        "55000" // object_not_in_prerequisite_state
    } else if err_str.ends_with("is not a sequence")
        || err_str.ends_with("is not a table")
        || err_str.starts_with("FILTER specified, but")
    {
        "42809" // wrong_object_type
    } else if err_str.starts_with("non-integer constant in") {
        "42601" // syntax_error
    } else if err_str == "LIMIT must not be negative" || err_str == "FETCH must not be negative" {
        "2201W" // invalid_row_count_in_limit_clause
    } else if err_str == "OFFSET must not be negative" {
        "2201X" // invalid_row_count_in_result_offset_clause
    } else if err_str.starts_with("unrecognized configuration parameter") {
        "42704" // undefined_object
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
    } else if err_str.starts_with("function ") && err_str.ends_with(" does not exist") {
        "42883" // undefined_function
    } else if err_str.contains("column") && err_str.contains("does not exist") {
        "42703" // undefined_column
    } else if err_str.contains("does not exist")
        || (err_str.starts_with("Table ") && err_str.ends_with(" not found"))
    {
        "42P01" // undefined_table (relation / index "x" does not exist)
    } else {
        "XX000" // internal_error
    }
}

/// The error for a statement that could not be planned. Errors PostgreSQL
/// also raises at this stage (an unknown column or function) keep their own
/// SQLSTATE; anything else is a construct NodusDB does not support.
pub(crate) fn planning_error(message: &str) -> PgWireError {
    match sqlstate_for_execution_error(message) {
        "XX000" => user_error("ERROR", "0A000", format!("Unsupported feature: {message}")),
        code => user_error("ERROR", code, message),
    }
}

/// For a settable `GUC_REPORT` variable, returns the canonical name PostgreSQL
/// uses in `ParameterStatus` messages (correct casing); `None` for variables
/// that are not reported to clients on change. Drivers (Npgsql's timezone
/// handling, pgjdbc's `standard_conforming_strings` tracking) rely on these
/// echoes after a `SET`.
pub(crate) fn reportable_guc_canonical_name(lower_name: &str) -> Option<&'static str> {
    Some(match lower_name {
        "application_name" => "application_name",
        "client_encoding" => "client_encoding",
        "datestyle" => "DateStyle",
        "intervalstyle" => "IntervalStyle",
        "timezone" => "TimeZone",
        "standard_conforming_strings" => "standard_conforming_strings",
        "default_transaction_read_only" => "default_transaction_read_only",
        _ => return None,
    })
}

/// Strips one surrounding layer of single/double quotes from a `SET` value so
/// the `ParameterStatus` echo carries the bare value (`'UTC'` -> `UTC`).
pub(crate) fn normalize_guc_value(raw: &str) -> String {
    let trimmed = raw.trim();
    let bytes = trimmed.as_bytes();
    if bytes.len() >= 2 {
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'\'' && last == b'\'') || (first == b'"' && last == b'"') {
            return trimmed[1..trimmed.len() - 1].to_string();
        }
    }
    trimmed.to_string()
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
            sqlstate_for_execution_error("Table missing_copy_table not found"),
            "42P01"
        );
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
    fn maps_shard_routing_failures() {
        for message in [
            "shard unavailable: shard-123 is not hosted on this node; retry after replica reconciliation",
            "shard routing changed for table 123; retry the transaction",
        ] {
            assert_eq!(sqlstate_for_execution_error(message), "40001");
        }
        for message in [
            "unsupported linearizable cross-shard range read: shared snapshot coordination is not implemented",
            "unsupported row scan spanning multiple tables; scan each table separately",
        ] {
            assert_eq!(sqlstate_for_execution_error(message), "0A000");
        }
        assert_eq!(
            sqlstate_for_execution_error("invalid shard map: gap or overlap"),
            "XX000"
        );
    }

    #[test]
    fn maps_dml_shape_errors() {
        for (message, code) in [
            ("INSERT has more expressions than target columns", "42601"),
            ("INSERT has more target columns than expressions", "42601"),
            ("column \"a\" specified more than once", "42701"),
            (
                "there is no unique or exclusion constraint matching the ON CONFLICT specification",
                "42P10",
            ),
            (
                "ON CONFLICT DO UPDATE command cannot affect row a second time",
                "21000",
            ),
            ("column \"nope\" of relation \"t\" does not exist", "42703"),
        ] {
            assert_eq!(sqlstate_for_execution_error(message), code, "{message}");
        }
    }

    #[test]
    fn maps_write_conflicts_to_serialization_failure() {
        for message in [
            "Write-write conflict detected on key. Transaction aborted.",
            "write conflict; retry transaction",
            "write-write conflict for transaction TxnId(0)",
            "stale epoch or fenced participant; retry transaction",
        ] {
            assert_eq!(sqlstate_for_execution_error(message), "40001");
        }
    }

    #[test]
    fn falls_back_to_internal_error() {
        assert_eq!(
            sqlstate_for_execution_error("something nobody classified yet"),
            "XX000"
        );
    }
}
