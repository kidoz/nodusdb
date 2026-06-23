//! Mapping from declared SQL type names to PostgreSQL wire types and type
//! modifiers (`typmod`), including array element types.

use crate::POSTGRES_TYPEMOD_NONE;
use pgwire::api::Type;

#[derive(Debug, Clone)]
pub(crate) struct PgDeclaredType {
    pub(crate) ty: Type,
    pub(crate) typmod: i32,
}

fn normalize_type_name(data_type: &str) -> String {
    data_type
        .trim()
        .trim_matches('"')
        .to_ascii_uppercase()
        .replace("CHARACTER VARYING", "VARCHAR")
        .replace("DOUBLE PRECISION", "DOUBLE")
        .replace("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ")
        .replace("TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP")
        .replace("TIME WITH TIME ZONE", "TIMETZ")
        .replace("TIME WITHOUT TIME ZONE", "TIME")
}

fn type_base_and_args(normalized: &str) -> (&str, Option<&str>) {
    let Some(open) = normalized.find('(') else {
        return (normalized.trim(), None);
    };
    let close = normalized.rfind(')').unwrap_or(normalized.len());
    (
        normalized[..open].trim(),
        Some(normalized[open + 1..close].trim()),
    )
}

fn numeric_typmod(args: Option<&str>) -> i32 {
    let Some(args) = args else {
        return POSTGRES_TYPEMOD_NONE;
    };
    let mut parts = args.split(',').map(str::trim);
    let precision = parts.next().and_then(|v| v.parse::<i32>().ok());
    let scale = parts
        .next()
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);
    match precision {
        Some(precision) if precision > 0 && (0..=precision).contains(&scale) => {
            ((precision << 16) | scale) + 4
        }
        _ => POSTGRES_TYPEMOD_NONE,
    }
}

fn length_typmod(args: Option<&str>) -> i32 {
    args.and_then(|v| v.trim().parse::<i32>().ok())
        .filter(|v| *v > 0)
        .map(|len| len + 4)
        .unwrap_or(POSTGRES_TYPEMOD_NONE)
}

fn array_type_for_base(base: &str) -> Type {
    match base {
        "BOOL" | "BOOLEAN" => Type::BOOL_ARRAY,
        "BYTEA" => Type::BYTEA_ARRAY,
        "PG_CHAR" => Type::CHAR_ARRAY,
        "CHAR" | "CHARACTER" | "BPCHAR" => Type::BPCHAR_ARRAY,
        "DATE" => Type::DATE_ARRAY,
        "FLOAT4" | "REAL" => Type::FLOAT4_ARRAY,
        "FLOAT8" | "FLOAT" | "DOUBLE" => Type::FLOAT8_ARRAY,
        "INT2" | "SMALLINT" | "SMALLSERIAL" => Type::INT2_ARRAY,
        "INT4" | "INT" | "INTEGER" | "SERIAL" => Type::INT4_ARRAY,
        "INT8" | "BIGINT" | "BIGSERIAL" => Type::INT8_ARRAY,
        "JSON" => Type::JSON_ARRAY,
        "JSONB" => Type::JSONB_ARRAY,
        "NAME" => Type::NAME_ARRAY,
        "NUMERIC" | "DECIMAL" => Type::NUMERIC_ARRAY,
        "OID" => Type::OID_ARRAY,
        "REGTYPE" => Type::REGTYPE_ARRAY,
        "TEXT" => Type::TEXT_ARRAY,
        "TIME" => Type::TIME_ARRAY,
        "TIMESTAMP" => Type::TIMESTAMP_ARRAY,
        "TIMESTAMPTZ" => Type::TIMESTAMPTZ_ARRAY,
        "UUID" => Type::UUID_ARRAY,
        "VARCHAR" | "CHARACTER VARYING" => Type::VARCHAR_ARRAY,
        _ => Type::TEXT_ARRAY,
    }
}

pub(crate) fn map_declared_type(data_type: &str) -> PgDeclaredType {
    let normalized = normalize_type_name(data_type);
    let is_array = normalized.ends_with("[]");
    let normalized = normalized.trim_end_matches("[]").trim();
    let (base, args) = type_base_and_args(normalized);
    if is_array {
        return PgDeclaredType {
            ty: array_type_for_base(base),
            typmod: POSTGRES_TYPEMOD_NONE,
        };
    }

    let ty = match base {
        "BOOL" | "BOOLEAN" => Type::BOOL,
        "BYTEA" => Type::BYTEA,
        "PG_CHAR" => Type::CHAR,
        "CHAR" | "CHARACTER" | "BPCHAR" => Type::BPCHAR,
        "DATE" => Type::DATE,
        "FLOAT4" | "REAL" => Type::FLOAT4,
        "FLOAT8" | "FLOAT" | "DOUBLE" => Type::FLOAT8,
        "INT2" | "SMALLINT" | "SMALLSERIAL" => Type::INT2,
        "INT4" | "INT" | "INTEGER" | "SERIAL" => Type::INT4,
        "INT8" | "BIGINT" | "BIGSERIAL" => Type::INT8,
        "JSON" => Type::JSON,
        "JSONB" => Type::JSONB,
        "NAME" => Type::NAME,
        "NUMERIC" | "DECIMAL" => Type::NUMERIC,
        "OID" => Type::OID,
        "REGCLASS" => Type::REGCLASS,
        "REGCONFIG" => Type::REGCONFIG,
        "REGDICTIONARY" => Type::REGDICTIONARY,
        "REGNAMESPACE" => Type::REGNAMESPACE,
        "REGOPER" => Type::REGOPER,
        "REGOPERATOR" => Type::REGOPERATOR,
        "REGPROC" => Type::REGPROC,
        "REGPROCEDURE" => Type::REGPROCEDURE,
        "REGROLE" => Type::REGROLE,
        "REGTYPE" => Type::REGTYPE,
        "TEXT" => Type::TEXT,
        "TIME" => Type::TIME,
        "TIMETZ" => Type::TIMETZ,
        "TIMESTAMP" => Type::TIMESTAMP,
        "TIMESTAMPTZ" => Type::TIMESTAMPTZ,
        "UUID" => Type::UUID,
        "VARCHAR" => Type::VARCHAR,
        _ => Type::TEXT,
    };
    let typmod = match ty {
        Type::BPCHAR | Type::VARCHAR => length_typmod(args),
        Type::NUMERIC => numeric_typmod(args),
        _ => POSTGRES_TYPEMOD_NONE,
    };
    PgDeclaredType { ty, typmod }
}

pub(crate) fn map_type(data_type: &str) -> Type {
    map_declared_type(data_type).ty
}
