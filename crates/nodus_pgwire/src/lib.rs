use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Error as IoError;
use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures_util::{Sink, SinkExt, StreamExt, stream};
use postgres_types::{IsNull, Json, Kind, ToSql};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tracing::{error, info};

use nodus_catalog::PrincipalId;
use nodus_security::{Authenticator, PasswordAuthenticator, Session, SessionRegistry};
use pgwire::api::auth::{
    DefaultServerParameterProvider, LoginInfo, ServerParameterProvider, StartupHandler,
    save_startup_parameters_to_metadata,
};
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::tokio::PgWireMessageServerCodec;

const METADATA_NODUS_SESSION_ID: &str = "nodus_session_id";
const METADATA_NODUS_PRINCIPAL_ID: &str = "nodus_principal_id";
const METADATA_BACKEND_PID: &str = "nodus_backend_pid";
const METADATA_BACKEND_SECRET: &str = "nodus_backend_secret";
const METADATA_TX_STATUS: &str = "nodus_tx_status";
const METADATA_COPY_ROWS: &str = "nodus_copy_rows";
const METADATA_COPY_EXTENDED: &str = "nodus_copy_extended";
const METADATA_STATEMENT_TIMEOUT_MS: &str = "nodus_statement_timeout_ms";

const POSTGRES_TYPEMOD_NONE: i32 = -1;

const CANCEL_REQUEST_MAGIC: i32 = 80877102;
const SSL_REQUEST_MAGIC: i32 = 80877103;
const GSSENC_REQUEST_MAGIC: i32 = 80877104;
const STARTUP_PACKET_HEADER_LEN: usize = 8;
const CANCEL_REQUEST_LEN: usize = 16;

#[derive(Debug, Clone)]
struct PgDeclaredType {
    ty: Type,
    typmod: i32,
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

fn map_declared_type(data_type: &str) -> PgDeclaredType {
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

fn map_type(data_type: &str) -> Type {
    map_declared_type(data_type).ty
}

/// Maps executor error text to the closest SQLSTATE so clients can react to
/// the failure class (constraint handling, missing-relation fallbacks during
/// introspection) instead of treating every error as an internal server fault.
fn sqlstate_for_execution_error(err_str: &str) -> &'static str {
    if err_str.contains("Unique constraint violation") {
        "23505"
    } else if err_str.contains("cannot be NULL") {
        "23502"
    } else if err_str.contains("relation \"") && err_str.contains("does not exist") {
        "42P01"
    } else {
        "XX000"
    }
}

fn user_error(severity: &str, code: &str, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        severity.to_owned(),
        code.to_owned(),
        message.into(),
    )))
}

fn session_id_from_client<C: ClientInfo>(client: &C) -> String {
    client
        .metadata()
        .get(METADATA_NODUS_SESSION_ID)
        .cloned()
        .unwrap_or_default()
}

fn principal_id_from_client<C: ClientInfo>(client: &C) -> PrincipalId {
    client
        .metadata()
        .get(METADATA_NODUS_PRINCIPAL_ID)
        .and_then(|s| Uuid::parse_str(s).ok())
        .map(PrincipalId)
        .unwrap_or_default()
}

fn tx_status_from_client<C: ClientInfo>(client: &C) -> TransactionStatus {
    match client
        .metadata()
        .get(METADATA_TX_STATUS)
        .map(String::as_str)
    {
        Some("T") => TransactionStatus::Transaction,
        Some("E") => TransactionStatus::Error,
        _ => TransactionStatus::Idle,
    }
}

fn set_tx_status<C: ClientInfo>(client: &mut C, status: TransactionStatus) {
    let encoded = match status {
        TransactionStatus::Idle => "I",
        TransactionStatus::Transaction => "T",
        TransactionStatus::Error => "E",
    };
    client
        .metadata_mut()
        .insert(METADATA_TX_STATUS.to_owned(), encoded.to_owned());
}

fn mark_error_status<C: ClientInfo>(client: &mut C) {
    if tx_status_from_client(client) == TransactionStatus::Transaction {
        set_tx_status(client, TransactionStatus::Error);
    }
}

fn apply_command_tag_to_tx_status<C: ClientInfo>(client: &mut C, tag: &str) {
    let command = tag.split_whitespace().next().unwrap_or(tag);
    if command.eq_ignore_ascii_case("BEGIN") {
        set_tx_status(client, TransactionStatus::Transaction);
    } else if command.eq_ignore_ascii_case("COMMIT") || tag.trim().eq_ignore_ascii_case("ROLLBACK")
    {
        set_tx_status(client, TransactionStatus::Idle);
    }
}

fn parse_statement_timeout_ms(query: &str) -> Option<u64> {
    let normalized = query
        .trim()
        .trim_end_matches(';')
        .replace('=', " = ")
        .replace(',', " ");
    let parts = normalized.split_whitespace().collect::<Vec<_>>();
    if parts.len() < 3
        || !parts[0].eq_ignore_ascii_case("SET")
        || !parts[1].eq_ignore_ascii_case("statement_timeout")
    {
        return None;
    }
    parts
        .iter()
        .skip(2)
        .find_map(|part| part.trim_matches('\'').parse::<u64>().ok())
}

fn remember_statement_timeout<C: ClientInfo>(client: &mut C, query: &str) {
    if let Some(timeout_ms) = parse_statement_timeout_ms(query) {
        client.metadata_mut().insert(
            METADATA_STATEMENT_TIMEOUT_MS.to_owned(),
            timeout_ms.to_string(),
        );
    }
}

fn statement_timeout_ms<C: ClientInfo>(client: &C) -> Option<u64> {
    client
        .metadata()
        .get(METADATA_STATEMENT_TIMEOUT_MS)
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
}

fn pg_sleep_ms(query: &str) -> Option<u64> {
    let lower = query.to_ascii_lowercase();
    let start = lower.find("pg_sleep(")? + "pg_sleep(".len();
    let rest = &lower[start..];
    let end = rest.find(')')?;
    let seconds = rest[..end].trim().parse::<f64>().ok()?;
    Some((seconds * 1000.0).ceil() as u64)
}

fn statement_would_timeout<C: ClientInfo>(client: &C, query: &str) -> bool {
    match (statement_timeout_ms(client), pg_sleep_ms(query)) {
        (Some(timeout_ms), Some(sleep_ms)) => sleep_ms >= timeout_ms,
        _ => false,
    }
}

fn is_set_default_statement(query: &str) -> bool {
    let q = query.trim().to_ascii_uppercase();
    q.starts_with("SET ") && q.contains(" DEFAULT")
}

fn described_statement_key(statement: &str) -> String {
    format!("nodus_described_statement:{statement}")
}

fn described_portal_key(portal_name: &str) -> String {
    format!("nodus_described_portal:{portal_name}")
}

fn command_tag_from_output_tag(output_tag: &str) -> Tag {
    if let Some(rest) = output_tag.strip_prefix("INSERT 0 ") {
        let rows = rest.parse::<usize>().unwrap_or(0);
        Tag::new("INSERT 0").with_rows(rows)
    } else if let Some(rest) = output_tag.strip_prefix("UPDATE ") {
        let rows = rest.parse::<usize>().unwrap_or(0);
        Tag::new("UPDATE").with_rows(rows)
    } else if let Some(rest) = output_tag.strip_prefix("DELETE ") {
        let rows = rest.parse::<usize>().unwrap_or(0);
        Tag::new("DELETE").with_rows(rows)
    } else {
        Tag::new(output_tag)
    }
}

fn row_description(fields: &[FieldInfo]) -> RowDescription {
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

fn row_description_from_metadata(
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

fn is_copy_from_stdin(query: &str) -> bool {
    let q = query.trim().to_ascii_uppercase();
    q.starts_with("COPY ") && q.contains(" FROM STDIN")
}

fn is_copy_to_stdout(query: &str) -> bool {
    let q = query.trim().to_ascii_uppercase();
    q.starts_with("COPY ") && q.contains(" TO STDOUT")
}

fn copy_format_code(query: &str) -> i8 {
    let q = query.trim().to_ascii_uppercase();
    if q.contains("FORMAT BINARY") || q.contains("WITH BINARY") {
        1
    } else {
        0
    }
}

fn copy_column_count(query: &str) -> i16 {
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

fn type_size(ty: &Type) -> i16 {
    match *ty {
        Type::BOOL => 1,
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

fn supports_binary_result(ty: &Type) -> bool {
    !matches!(
        *ty,
        Type::NUMERIC
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

fn effective_result_format(ty: &Type, requested: FieldFormat) -> FieldFormat {
    if requested == FieldFormat::Binary && supports_binary_result(ty) {
        FieldFormat::Binary
    } else {
        FieldFormat::Text
    }
}

fn field_info_for_output(
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

fn render_scalar_text(value: &nodus_executor::Value, declared: &Type) -> String {
    match value {
        nodus_executor::Value::Int(i) => i.to_string(),
        nodus_executor::Value::Float(f) => f.to_string(),
        nodus_executor::Value::Text(s) if *declared == Type::BYTEA => render_bytea_text(s),
        nodus_executor::Value::Text(s) => s.clone(),
        nodus_executor::Value::Bool(b) => {
            if *b {
                "t".to_owned()
            } else {
                "f".to_owned()
            }
        }
        nodus_executor::Value::Array(arr) => render_array_text(arr),
        nodus_executor::Value::Jsonb(j) => j.to_string(),
        nodus_executor::Value::Null => String::new(),
    }
}

fn render_bytea_text(raw: &str) -> String {
    if raw.starts_with("\\x") {
        raw.to_owned()
    } else {
        format!("\\x{}", hex_encode(raw.as_bytes()))
    }
}

fn render_array_text(arr: &[nodus_executor::Value]) -> String {
    let rendered = arr
        .iter()
        .map(|v| match v {
            nodus_executor::Value::Null => "NULL".to_owned(),
            nodus_executor::Value::Array(nested) => render_array_text(nested),
            _ => quote_array_value(&render_scalar_text(v, &Type::TEXT)),
        })
        .collect::<Vec<_>>();
    format!("{{{}}}", rendered.join(","))
}

fn quote_array_value(value: &str) -> String {
    if value.is_empty()
        || value.eq_ignore_ascii_case("NULL")
        || value
            .bytes()
            .any(|b| matches!(b, b'"' | b'\\' | b',' | b'{' | b'}') || b.is_ascii_whitespace())
    {
        format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        value.to_owned()
    }
}

fn value_to_string(value: &nodus_executor::Value) -> String {
    render_scalar_text(value, &Type::TEXT)
}

fn parse_i64(value: &nodus_executor::Value) -> std::io::Result<i64> {
    match value {
        nodus_executor::Value::Int(i) => Ok(*i),
        nodus_executor::Value::Float(f) => Ok(*f as i64),
        _ => value_to_string(value)
            .parse::<i64>()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
    }
}

fn parse_f64(value: &nodus_executor::Value) -> std::io::Result<f64> {
    match value {
        nodus_executor::Value::Int(i) => Ok(*i as f64),
        nodus_executor::Value::Float(f) => Ok(*f),
        _ => value_to_string(value)
            .parse::<f64>()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
    }
}

fn parse_bool(value: &nodus_executor::Value) -> std::io::Result<bool> {
    match value {
        nodus_executor::Value::Bool(b) => Ok(*b),
        _ => match value_to_string(value).to_ascii_lowercase().as_str() {
            "t" | "true" | "1" | "yes" | "on" => Ok(true),
            "f" | "false" | "0" | "no" | "off" => Ok(false),
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid boolean value {other:?}"),
            )),
        },
    }
}

fn parse_json(value: &nodus_executor::Value) -> std::io::Result<serde_json::Value> {
    match value {
        nodus_executor::Value::Jsonb(j) => Ok(j.clone()),
        _ => serde_json::from_str(&value_to_string(value))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
    }
}

fn parse_uuid(value: &nodus_executor::Value) -> std::io::Result<Uuid> {
    Uuid::parse_str(&value_to_string(value))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn parse_date(value: &nodus_executor::Value) -> std::io::Result<NaiveDate> {
    NaiveDate::parse_from_str(&value_to_string(value), "%Y-%m-%d")
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn parse_time(value: &nodus_executor::Value) -> std::io::Result<NaiveTime> {
    let s = value_to_string(value);
    NaiveTime::parse_from_str(&s, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(&s, "%H:%M"))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn parse_timestamp(value: &nodus_executor::Value) -> std::io::Result<NaiveDateTime> {
    let s = value_to_string(value).replace('T', " ");
    NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f")
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn parse_timestamptz(value: &nodus_executor::Value) -> std::io::Result<DateTime<Utc>> {
    let s = value_to_string(value);
    if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
        return Ok(dt.with_timezone(&Utc));
    }
    if let Ok(dt) = DateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f%#z") {
        return Ok(dt.with_timezone(&Utc));
    }
    parse_timestamp(value).map(|naive| naive.and_utc())
}

fn parse_bytea(value: &nodus_executor::Value) -> std::io::Result<Vec<u8>> {
    let raw = value_to_string(value);
    let Some(hex) = raw.strip_prefix("\\x").or_else(|| raw.strip_prefix("\\X")) else {
        return Ok(raw.into_bytes());
    };
    hex_decode(hex)
}

fn hex_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(TABLE[(byte >> 4) as usize] as char);
        out.push(TABLE[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(hex: &str) -> std::io::Result<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "odd-length bytea hex value",
        ));
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    for pair in bytes.chunks_exact(2) {
        let high = hex_value(pair[0])?;
        let low = hex_value(pair[1])?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn hex_value(byte: u8) -> std::io::Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid bytea hex digit",
        )),
    }
}

fn append_tosql<T: ToSql>(row: &mut BytesMut, ty: &Type, value: &T) -> std::io::Result<()> {
    let mut encoded = BytesMut::new();
    let is_null = value
        .to_sql(ty, &mut encoded)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    match is_null {
        IsNull::Yes => row.put_i32(-1),
        IsNull::No => {
            row.put_i32(encoded.len() as i32);
            row.put_slice(&encoded);
        }
    }
    Ok(())
}

fn append_text(row: &mut BytesMut, text: String) {
    row.put_i32(text.len() as i32);
    row.put_slice(text.as_bytes());
}

fn array_values(value: &nodus_executor::Value) -> std::io::Result<&[nodus_executor::Value]> {
    match value {
        nodus_executor::Value::Array(values) => Ok(values),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "expected array value",
        )),
    }
}

fn encode_array_binary(
    row: &mut BytesMut,
    value: &nodus_executor::Value,
    declared: &Type,
) -> std::io::Result<bool> {
    let values = array_values(value)?;
    match *declared {
        Type::BOOL_ARRAY => append_tosql(
            row,
            declared,
            &values
                .iter()
                .map(optional_bool)
                .collect::<std::io::Result<Vec<_>>>()?,
        ),
        Type::BYTEA_ARRAY => append_tosql(
            row,
            declared,
            &values
                .iter()
                .map(optional_bytea)
                .collect::<std::io::Result<Vec<_>>>()?,
        ),
        Type::DATE_ARRAY => append_tosql(
            row,
            declared,
            &values
                .iter()
                .map(optional_date)
                .collect::<std::io::Result<Vec<_>>>()?,
        ),
        Type::FLOAT4_ARRAY => append_tosql(
            row,
            declared,
            &values
                .iter()
                .map(optional_f32)
                .collect::<std::io::Result<Vec<_>>>()?,
        ),
        Type::FLOAT8_ARRAY => append_tosql(
            row,
            declared,
            &values
                .iter()
                .map(optional_f64)
                .collect::<std::io::Result<Vec<_>>>()?,
        ),
        Type::INT2_ARRAY => append_tosql(
            row,
            declared,
            &values
                .iter()
                .map(optional_i16)
                .collect::<std::io::Result<Vec<_>>>()?,
        ),
        Type::INT4_ARRAY => append_tosql(
            row,
            declared,
            &values
                .iter()
                .map(optional_i32)
                .collect::<std::io::Result<Vec<_>>>()?,
        ),
        Type::INT8_ARRAY => append_tosql(
            row,
            declared,
            &values
                .iter()
                .map(optional_i64)
                .collect::<std::io::Result<Vec<_>>>()?,
        ),
        Type::JSON_ARRAY | Type::JSONB_ARRAY => append_tosql(
            row,
            declared,
            &values
                .iter()
                .map(optional_json)
                .collect::<std::io::Result<Vec<_>>>()?,
        ),
        Type::TEXT_ARRAY | Type::VARCHAR_ARRAY | Type::BPCHAR_ARRAY | Type::NAME_ARRAY => {
            append_tosql(
                row,
                declared,
                &values.iter().map(optional_string).collect::<Vec<_>>(),
            )
        }
        Type::TIME_ARRAY => append_tosql(
            row,
            declared,
            &values
                .iter()
                .map(optional_time)
                .collect::<std::io::Result<Vec<_>>>()?,
        ),
        Type::TIMESTAMP_ARRAY => append_tosql(
            row,
            declared,
            &values
                .iter()
                .map(optional_timestamp)
                .collect::<std::io::Result<Vec<_>>>()?,
        ),
        Type::TIMESTAMPTZ_ARRAY => append_tosql(
            row,
            declared,
            &values
                .iter()
                .map(optional_timestamptz)
                .collect::<std::io::Result<Vec<_>>>()?,
        ),
        Type::UUID_ARRAY => append_tosql(
            row,
            declared,
            &values
                .iter()
                .map(optional_uuid)
                .collect::<std::io::Result<Vec<_>>>()?,
        ),
        _ => return Ok(false),
    }?;
    Ok(true)
}

fn optional_string(value: &nodus_executor::Value) -> Option<String> {
    if matches!(value, nodus_executor::Value::Null) {
        None
    } else {
        Some(value_to_string(value))
    }
}

fn optional_bool(value: &nodus_executor::Value) -> std::io::Result<Option<bool>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_bool(value).map(Some)
    }
}

fn optional_bytea(value: &nodus_executor::Value) -> std::io::Result<Option<Vec<u8>>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_bytea(value).map(Some)
    }
}

fn optional_date(value: &nodus_executor::Value) -> std::io::Result<Option<NaiveDate>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_date(value).map(Some)
    }
}

fn optional_time(value: &nodus_executor::Value) -> std::io::Result<Option<NaiveTime>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_time(value).map(Some)
    }
}

fn optional_timestamp(value: &nodus_executor::Value) -> std::io::Result<Option<NaiveDateTime>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_timestamp(value).map(Some)
    }
}

fn optional_timestamptz(value: &nodus_executor::Value) -> std::io::Result<Option<DateTime<Utc>>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_timestamptz(value).map(Some)
    }
}

fn optional_uuid(value: &nodus_executor::Value) -> std::io::Result<Option<Uuid>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_uuid(value).map(Some)
    }
}

fn optional_json(
    value: &nodus_executor::Value,
) -> std::io::Result<Option<Json<serde_json::Value>>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_json(value).map(Json).map(Some)
    }
}

fn optional_i16(value: &nodus_executor::Value) -> std::io::Result<Option<i16>> {
    optional_i64(value).map(|v| v.map(|n| n as i16))
}

fn optional_i32(value: &nodus_executor::Value) -> std::io::Result<Option<i32>> {
    optional_i64(value).map(|v| v.map(|n| n as i32))
}

fn optional_i64(value: &nodus_executor::Value) -> std::io::Result<Option<i64>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_i64(value).map(Some)
    }
}

fn optional_f32(value: &nodus_executor::Value) -> std::io::Result<Option<f32>> {
    optional_f64(value).map(|v| v.map(|n| n as f32))
}

fn optional_f64(value: &nodus_executor::Value) -> std::io::Result<Option<f64>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_f64(value).map(Some)
    }
}

fn append_value(
    row: &mut BytesMut,
    value: &nodus_executor::Value,
    declared: &Type,
    format: FieldFormat,
) -> std::io::Result<()> {
    if matches!(value, nodus_executor::Value::Null) {
        row.put_i32(-1);
        return Ok(());
    }
    if format == FieldFormat::Text {
        append_text(row, render_scalar_text(value, declared));
        return Ok(());
    }

    match *declared {
        Type::BOOL => append_tosql(row, declared, &parse_bool(value)?),
        Type::BYTEA => append_tosql(row, declared, &parse_bytea(value)?),
        Type::DATE => append_tosql(row, declared, &parse_date(value)?),
        Type::FLOAT4 => append_tosql(row, declared, &(parse_f64(value)? as f32)),
        Type::FLOAT8 => append_tosql(row, declared, &parse_f64(value)?),
        Type::INT2 => append_tosql(row, declared, &(parse_i64(value)? as i16)),
        Type::INT4 => append_tosql(row, declared, &(parse_i64(value)? as i32)),
        Type::INT8 => append_tosql(row, declared, &parse_i64(value)?),
        Type::JSON | Type::JSONB => append_tosql(row, declared, &Json(parse_json(value)?)),
        Type::NAME | Type::TEXT | Type::VARCHAR | Type::BPCHAR => {
            append_tosql(row, declared, &value_to_string(value))
        }
        Type::OID => append_tosql(row, declared, &(parse_i64(value)? as u32)),
        Type::TIME => append_tosql(row, declared, &parse_time(value)?),
        Type::TIMESTAMP => append_tosql(row, declared, &parse_timestamp(value)?),
        Type::TIMESTAMPTZ => append_tosql(row, declared, &parse_timestamptz(value)?),
        Type::UUID => append_tosql(row, declared, &parse_uuid(value)?),
        _ if matches!(declared.kind(), Kind::Array(_))
            && encode_array_binary(row, value, declared)? =>
        {
            Ok(())
        }
        _ => {
            append_text(row, render_scalar_text(value, declared));
            Ok(())
        }
    }
}

fn encode_row(
    values: &[nodus_executor::Value],
    field_info: Arc<Vec<FieldInfo>>,
) -> std::io::Result<DataRow> {
    let mut row = BytesMut::new();
    for (i, value) in values.iter().enumerate() {
        let Some(field) = field_info.get(i) else {
            break;
        };
        append_value(&mut row, value, field.datatype(), field.format())?;
    }
    Ok(DataRow::new(row, values.len() as i16))
}

fn parse_text_array_parameter(raw: &str) -> nodus_executor::Value {
    let inner = raw
        .trim()
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(raw);
    let values = if inner.is_empty() {
        Vec::new()
    } else {
        inner
            .split(',')
            .map(|part| {
                let value = part.trim().trim_matches('"').replace("\\\"", "\"");
                if value.eq_ignore_ascii_case("NULL") {
                    nodus_executor::Value::Null
                } else {
                    nodus_executor::Value::Text(value)
                }
            })
            .collect()
    };
    nodus_executor::Value::Array(values)
}

fn text_parameter_value(param_type: &Type, raw: String) -> nodus_executor::Value {
    match *param_type {
        Type::BOOL => nodus_executor::Value::Bool(matches!(
            raw.to_ascii_lowercase().as_str(),
            "t" | "true" | "1" | "yes" | "on"
        )),
        Type::INT2 | Type::INT4 | Type::INT8 | Type::OID => raw
            .parse::<i64>()
            .map(nodus_executor::Value::Int)
            .unwrap_or(nodus_executor::Value::Null),
        Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC => raw
            .parse::<f64>()
            .map(nodus_executor::Value::Float)
            .unwrap_or(nodus_executor::Value::Null),
        Type::JSON | Type::JSONB => serde_json::from_str(&raw)
            .map(nodus_executor::Value::Jsonb)
            .unwrap_or(nodus_executor::Value::Text(raw)),
        _ if matches!(param_type.kind(), Kind::Array(_)) => parse_text_array_parameter(&raw),
        _ => nodus_executor::Value::Text(raw),
    }
}

fn values_to_array<T, F>(values: Vec<Option<T>>, render: F) -> nodus_executor::Value
where
    F: Fn(T) -> nodus_executor::Value,
{
    nodus_executor::Value::Array(
        values
            .into_iter()
            .map(|value| value.map(&render).unwrap_or(nodus_executor::Value::Null))
            .collect(),
    )
}

use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, DEFAULT_NAME, DefaultClient, PgWireConnectionState,
    PgWireHandlerFactory, Type,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::data::{
    DataRow, FieldDescription, NoData, ParameterDescription, RowDescription,
};
use pgwire::messages::extendedquery::{
    Bind, BindComplete, Close, CloseComplete, Describe, Execute, Parse, ParseComplete,
    PortalSuspended, Sync as PgSync, TARGET_TYPE_BYTE_PORTAL, TARGET_TYPE_BYTE_STATEMENT,
};
use pgwire::messages::response::{
    CommandComplete, EmptyQueryResponse, ErrorResponse, ReadyForQuery, SslResponse,
    TransactionStatus,
};
use pgwire::messages::startup::{Authentication, BackendKeyData, ParameterStatus};
use uuid::Uuid;

pub struct NodusQueryHandler {
    /// Fallback session id for tests that instantiate the handler directly.
    pub session_id: String,
    pub session_state: nodus_sql::SessionState,
    pub executor: Arc<dyn nodus_executor::Executor>,
    pub metrics: nodus_monitoring::Metrics,
    registry: Arc<SessionRegistry>,
    slow_log: Arc<nodus_monitoring::SlowQueryLog>,
}

/// Observes query latency on drop, so every `do_query` return path is covered:
/// records into the histogram and, if slow, into the slow-query log.
struct QueryTimer<'a> {
    start: std::time::Instant,
    sql: &'a str,
    session_id: &'a str,
    metrics: &'a nodus_monitoring::Metrics,
    slow_log: &'a nodus_monitoring::SlowQueryLog,
}

impl Drop for QueryTimer<'_> {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        self.metrics
            .query_latency_seconds
            .observe(elapsed.as_secs_f64());
        let ms = elapsed.as_millis() as u64;
        if ms >= self.slow_log.threshold_ms() {
            self.slow_log
                .record(self.sql.to_string(), ms, self.session_id.to_string());
            self.metrics.slow_queries_total.inc();
        }
    }
}

struct CurrentQueryGuard<'a> {
    registry: &'a SessionRegistry,
    session_id: &'a str,
}

impl Drop for CurrentQueryGuard<'_> {
    fn drop(&mut self) {
        self.registry.finish_current_query(self.session_id);
    }
}

impl Drop for NodusQueryHandler {
    fn drop(&mut self) {
        if !self.session_id.is_empty() {
            self.registry.deregister(&self.session_id);
        }
    }
}

/// Startup handler that authenticates clients with cleartext passwords against
/// the [`PasswordAuthenticator`]. On success the principal id is stashed in the
/// connection metadata for downstream authorization.
pub struct NodusStartupHandler {
    authenticator: Arc<PasswordAuthenticator>,
    param_provider: DefaultServerParameterProvider,
    registry: Arc<SessionRegistry>,
}

async fn finish_nodus_authentication<C>(
    client: &mut C,
    server_parameter_provider: &DefaultServerParameterProvider,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    client
        .send(PgWireBackendMessage::Authentication(Authentication::Ok))
        .await?;

    if let Some(mut parameters) = server_parameter_provider.server_parameters(client) {
        parameters.insert("server_version_num".to_owned(), "160000".to_owned());
        parameters.insert("TimeZone".to_owned(), "UTC".to_owned());
        parameters.insert("IntervalStyle".to_owned(), "postgres".to_owned());
        parameters.insert("standard_conforming_strings".to_owned(), "on".to_owned());
        parameters.insert("is_superuser".to_owned(), "on".to_owned());
        parameters.insert("session_authorization".to_owned(), "nodus".to_owned());
        let app = client
            .metadata()
            .get("application_name")
            .cloned()
            .unwrap_or_default();
        parameters.insert("application_name".to_owned(), app);
        let mut parameters: Vec<_> = parameters.into_iter().collect();
        parameters.sort_by(|a, b| a.0.cmp(&b.0));
        for (name, value) in parameters {
            client
                .send(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                    name, value,
                )))
                .await?;
        }
    }

    let pid = client
        .metadata()
        .get(METADATA_BACKEND_PID)
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(std::process::id() as i32);
    let secret = client
        .metadata()
        .get(METADATA_BACKEND_SECRET)
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or_default();
    client
        .send(PgWireBackendMessage::BackendKeyData(BackendKeyData::new(
            pid, secret,
        )))
        .await?;
    client
        .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
            tx_status_from_client(client),
        )))
        .await?;
    client.flush().await?;
    client.set_state(PgWireConnectionState::ReadyForQuery);
    Ok(())
}

#[async_trait]
impl StartupHandler for NodusStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: pgwire::messages::PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            pgwire::messages::PgWireFrontendMessage::Startup(ref startup) => {
                save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let pwd = pwd.into_password()?;
                let login = LoginInfo::from_client_info(client);
                let user = login.user().map(|u| u.to_string()).unwrap_or_default();
                match self.authenticator.authenticate(&user, &pwd.password) {
                    Ok(session) => {
                        let session_id = session_id_from_client(client);
                        self.registry
                            .update_principal(&session_id, session.principal_id);
                        client.metadata_mut().insert(
                            METADATA_NODUS_PRINCIPAL_ID.to_string(),
                            session.principal_id.to_string(),
                        );
                        finish_nodus_authentication(client, &self.param_provider).await?;
                    }
                    Err(_) => {
                        let error_info = ErrorInfo::new(
                            "FATAL".to_owned(),
                            "28P01".to_owned(),
                            "password authentication failed".to_owned(),
                        );
                        client
                            .feed(PgWireBackendMessage::ErrorResponse(ErrorResponse::from(
                                error_info,
                            )))
                            .await?;
                        client.close().await?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for NodusQueryHandler {
    async fn on_query<C>(
        &self,
        client: &mut C,
        query: pgwire::messages::simplequery::Query,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client.set_state(PgWireConnectionState::QueryInProgress);
        let query_string = query.query;
        if query_string.trim().is_empty() || query_string.trim() == ";" {
            client
                .feed(PgWireBackendMessage::EmptyQueryResponse(
                    EmptyQueryResponse::new(),
                ))
                .await?;
        } else if is_copy_from_stdin(&query_string) {
            let session_id = session_id_from_client(client);
            self.registry.set_current_query(&session_id, &query_string);
            client.metadata_mut().extend([
                (METADATA_COPY_ROWS.to_owned(), "0".to_owned()),
                (METADATA_COPY_EXTENDED.to_owned(), "0".to_owned()),
            ]);
            client
                .send(PgWireBackendMessage::CopyInResponse(
                    pgwire::messages::copy::CopyInResponse::new(
                        copy_format_code(&query_string),
                        copy_column_count(&query_string),
                        vec![
                            copy_format_code(&query_string) as i16;
                            copy_column_count(&query_string) as usize
                        ],
                    ),
                ))
                .await?;
            client.flush().await?;
            client.set_state(PgWireConnectionState::CopyInProgress);
            return Ok(());
        } else if is_copy_to_stdout(&query_string) {
            let session_id = session_id_from_client(client);
            self.registry.set_current_query(&session_id, &query_string);
            self.registry.finish_current_query(&session_id);
            client
                .send(PgWireBackendMessage::CopyOutResponse(
                    pgwire::messages::copy::CopyOutResponse::new(
                        copy_format_code(&query_string),
                        copy_column_count(&query_string),
                        vec![
                            copy_format_code(&query_string) as i16;
                            copy_column_count(&query_string) as usize
                        ],
                    ),
                ))
                .await?;
            client
                .send(PgWireBackendMessage::CopyDone(CopyDone::new()))
                .await?;
            client
                .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                    "COPY 0".to_owned(),
                )))
                .await?;
        } else {
            let resp = self.do_query(client, &query_string).await?;
            for r in resp {
                match r {
                    Response::EmptyQuery => {
                        client
                            .feed(PgWireBackendMessage::EmptyQueryResponse(
                                EmptyQueryResponse::new(),
                            ))
                            .await?;
                    }
                    Response::Query(results) => {
                        let row_desc = row_description(&results.row_schema());
                        client
                            .send(PgWireBackendMessage::RowDescription(row_desc))
                            .await?;
                        let command_tag = results.command_tag().to_owned();
                        let mut rows = 0;
                        let mut data_rows = results.data_rows();
                        while let Some(row) = data_rows.next().await {
                            rows += 1;
                            client.feed(PgWireBackendMessage::DataRow(row?)).await?;
                        }
                        client
                            .send(PgWireBackendMessage::CommandComplete(
                                Tag::new(&command_tag).with_rows(rows).into(),
                            ))
                            .await?;
                    }
                    Response::Execution(tag) => {
                        let command: CommandComplete = tag.into();
                        apply_command_tag_to_tx_status(client, &command.tag);
                        client
                            .send(PgWireBackendMessage::CommandComplete(command))
                            .await?;
                    }
                    Response::Error(e) => {
                        mark_error_status(client);
                        client
                            .send(PgWireBackendMessage::ErrorResponse((*e).into()))
                            .await?;
                    }
                    Response::CopyIn(_) | Response::CopyOut(_) | Response::CopyBoth(_) => {
                        return Err(user_error(
                            "ERROR",
                            "0A000",
                            "COPY response is only supported by COPY statements",
                        ));
                    }
                }
            }
        }

        client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                tx_status_from_client(client),
            )))
            .await?;
        client.flush().await?;
        client.set_state(PgWireConnectionState::ReadyForQuery);
        Ok(())
    }

    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        info!("Received query: {}", query);
        self.metrics.queries_total.inc();

        let session_id = {
            let id = session_id_from_client(client);
            if id.is_empty() {
                self.session_id.clone()
            } else {
                id
            }
        };

        // Honor administrative termination and PostgreSQL cancel requests.
        if self.registry.is_cancelled(&session_id) {
            self.metrics.query_errors_total.inc();
            return Err(user_error(
                "ERROR",
                "57P01",
                "session terminated by administrator",
            ));
        }
        if self.registry.is_query_cancelled(&session_id) {
            self.metrics.query_errors_total.inc();
            return Err(user_error(
                "ERROR",
                "57014",
                "canceling statement due to user request",
            ));
        }
        self.registry.set_current_query(&session_id, query);
        let _current_query = CurrentQueryGuard {
            registry: &self.registry,
            session_id: &session_id,
        };

        // OpenTelemetry span covering the statement (no-op unless OTLP is on).
        let _otel_span = nodus_telemetry::start_span("pgwire.simple_query");

        // Times the whole statement regardless of which branch returns.
        let _timer = QueryTimer {
            start: std::time::Instant::now(),
            sql: query,
            session_id: &session_id,
            metrics: &self.metrics,
            slow_log: &self.slow_log,
        };

        // The authenticated principal was stashed in connection metadata by the
        // startup handler; carry it into the execution context for authorization.
        let principal_id = principal_id_from_client(client);

        let ctx = nodus_executor::ExecutionContext {
            session_id: session_id.clone(),
            principal_id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };

        let query_str = query;
        remember_statement_timeout(client, query_str);
        if statement_would_timeout(client, query_str) {
            self.metrics.query_errors_total.inc();
            mark_error_status(client);
            return Err(user_error(
                "ERROR",
                "57014",
                "canceling statement due to statement timeout",
            ));
        }
        if is_set_default_statement(query_str) {
            return Ok(vec![Response::Execution(Tag::new("SET"))]);
        }

        // Parse SQL and translate every statement in a simple-query batch.
        // Drivers such as Npgsql issue startup metadata batches and expect one
        // protocol result for each statement before ReadyForQuery.
        let statements = match nodus_sql::parse_sql(query_str) {
            Ok(stmts) if !stmts.is_empty() => stmts,
            Ok(_) => return Ok(vec![Response::Execution(Tag::new("OK"))]),
            Err(e) => {
                error!("Failed to parse SQL: {}", e);
                self.metrics.query_errors_total.inc();
                let err = ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    format!("Syntax error: {}", e),
                );
                return Err(PgWireError::UserError(Box::new(err)));
            }
        };

        let mut responses = Vec::new();
        for stmt in statements {
            let plan = match nodus_executor::plan_statement(&stmt, &[]) {
                Ok(plan) => plan,
                Err(e) => {
                    error!("Failed to plan SQL: {}", e);
                    self.metrics.query_errors_total.inc();
                    let err = ErrorInfo::new(
                        "ERROR".to_owned(),
                        "0A000".to_owned(),
                        format!("Unsupported feature: {}", e),
                    );
                    return Err(PgWireError::UserError(Box::new(err)));
                }
            };

            let out = match self.executor.execute_logical(&ctx, plan) {
                Ok(out) => out,
                Err(e) => {
                    let err_str = e.to_string();
                    let code = sqlstate_for_execution_error(&err_str);
                    let err = ErrorInfo::new("ERROR".to_owned(), code.to_owned(), err_str);
                    mark_error_status(client);
                    return Err(PgWireError::UserError(Box::new(err)));
                }
            };

            if self.registry.is_query_cancelled(&session_id) {
                self.metrics.query_errors_total.inc();
                mark_error_status(client);
                return Err(user_error(
                    "ERROR",
                    "57014",
                    "canceling statement due to user request",
                ));
            }

            // No projected columns => a command tag (CREATE TABLE, INSERT, BEGIN...).
            if out.columns.is_empty() {
                apply_command_tag_to_tx_status(client, &out.tag);
                let tag = command_tag_from_output_tag(&out.tag);
                responses.push(Response::Execution(tag));
                continue;
            }

            // Otherwise a row set: build field descriptors and encode each row.
            let field_info =
                field_info_for_output(&out.columns, &out.types, |_, _| FieldFormat::Text);
            let mut data_rows = Vec::new();
            for row in &out.rows {
                data_rows.push(
                    encode_row(&row.values, field_info.clone()).map_err(PgWireError::IoError),
                );
            }
            responses.push(Response::Query(QueryResponse::new(
                field_info,
                stream::iter(data_rows),
            )));
        }
        Ok(responses)
    }
}

pub struct NodusExtendedQueryHandler {
    pub executor: Arc<dyn nodus_executor::Executor>,
    pub metrics: nodus_monitoring::Metrics,
    pub slow_log: Arc<nodus_monitoring::SlowQueryLog>,
    registry: Arc<SessionRegistry>,
    cursors: RwLock<HashMap<String, PortalCursor>>,
}

struct PortalCursor {
    fields: Arc<Vec<FieldInfo>>,
    rows: Vec<DataRow>,
    position: usize,
    total_rows: usize,
}

impl PortalCursor {
    fn next_chunk(&mut self, max_rows: usize) -> (Vec<DataRow>, bool) {
        let remaining = self.rows.len().saturating_sub(self.position);
        let take = if max_rows == 0 {
            remaining
        } else {
            remaining.min(max_rows)
        };
        let start = self.position;
        let end = start + take;
        self.position = end;
        let suspended = self.position < self.rows.len();
        (self.rows[start..end].to_vec(), suspended)
    }
}

fn cursor_key(session_id: &str, portal_name: &str) -> String {
    format!("{session_id}:{portal_name}")
}

impl NodusExtendedQueryHandler {
    fn output_metadata_for_query<C>(&self, client: &C, query_str: &str) -> Vec<(String, String)>
    where
        C: ClientInfo,
    {
        let session_id = session_id_from_client(client);
        let principal_id = principal_id_from_client(client);
        let ctx = nodus_executor::ExecutionContext {
            session_id,
            principal_id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };

        let Ok(mut stmts) = nodus_sql::parse_sql(query_str) else {
            return Vec::new();
        };
        let Some(parsed) = stmts.pop() else {
            return Vec::new();
        };
        let Ok(plan) = nodus_executor::plan_statement(&parsed, &[]) else {
            return Vec::new();
        };

        let mut plan_zero = plan.clone();
        let can_execute = match &mut plan_zero {
            nodus_executor::LogicalPlan::Select { limit, .. } => {
                *limit = Some(0);
                true
            }
            nodus_executor::LogicalPlan::ShowVariable { .. }
            | nodus_executor::LogicalPlan::SelectLiteral { .. } => true,
            _ => false,
        };
        if !can_execute {
            return Vec::new();
        }

        self.executor
            .execute_logical(&ctx, plan_zero)
            .map(|out| out.columns.into_iter().zip(out.types).collect())
            .unwrap_or_default()
    }
}

#[async_trait]
impl ExtendedQueryHandler for NodusExtendedQueryHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser::new())
    }

    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let types = message
            .type_oids
            .iter()
            .map(|oid| Type::from_oid(*oid).unwrap_or(Type::UNKNOWN))
            .collect::<Vec<Type>>();
        let stmt = StoredStatement::new(
            message.name.unwrap_or_else(|| DEFAULT_NAME.to_owned()),
            message.query,
            types,
        );
        client.portal_store().put_statement(Arc::new(stmt));
        client
            .send(PgWireBackendMessage::ParseComplete(ParseComplete::new()))
            .await?;
        Ok(())
    }

    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let statement_name = message.statement_name.as_deref().unwrap_or(DEFAULT_NAME);
        let Some(statement) = client.portal_store().get_statement(statement_name) else {
            return Err(PgWireError::StatementNotFound(statement_name.to_owned()));
        };
        let portal = Portal::try_new(&message, statement)?;
        let key = cursor_key(&session_id_from_client(client), &portal.name);
        self.cursors.write().unwrap().remove(&key);
        client.portal_store().put_portal(Arc::new(portal));
        client
            .send(PgWireBackendMessage::BindComplete(BindComplete::new()))
            .await?;
        Ok(())
    }

    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let portal_name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        let key = cursor_key(&session_id_from_client(client), portal_name);
        let max_rows = message.max_rows as usize;
        let existing = {
            let mut cursors = self.cursors.write().unwrap();
            if let Some(cursor) = cursors.get_mut(&key) {
                let (rows, suspended) = cursor.next_chunk(max_rows);
                let done = !suspended;
                let fields = cursor.fields.clone();
                let total_rows = cursor.total_rows;
                if done {
                    cursors.remove(&key);
                }
                Some((fields, rows, suspended, total_rows))
            } else {
                None
            }
        };
        if let Some((fields, rows, suspended, total_rows)) = existing {
            for row in rows {
                client.send(PgWireBackendMessage::DataRow(row)).await?;
            }
            if suspended {
                client
                    .send(PgWireBackendMessage::PortalSuspended(PortalSuspended::new()))
                    .await?;
            } else {
                client
                    .send(PgWireBackendMessage::CommandComplete(
                        Tag::new("SELECT").with_rows(total_rows).into(),
                    ))
                    .await?;
            }
            let _ = fields;
            return Ok(());
        }

        let Some(portal) = client.portal_store().get_portal(portal_name) else {
            return Err(PgWireError::PortalNotFound(portal_name.to_owned()));
        };

        if is_copy_from_stdin(&portal.statement.statement) {
            let session_id = session_id_from_client(client);
            self.registry
                .set_current_query(&session_id, &portal.statement.statement);
            client.metadata_mut().extend([
                (METADATA_COPY_ROWS.to_owned(), "0".to_owned()),
                (METADATA_COPY_EXTENDED.to_owned(), "1".to_owned()),
            ]);
            client
                .send(PgWireBackendMessage::CopyInResponse(
                    pgwire::messages::copy::CopyInResponse::new(
                        copy_format_code(&portal.statement.statement),
                        copy_column_count(&portal.statement.statement),
                        vec![
                            copy_format_code(&portal.statement.statement) as i16;
                            copy_column_count(&portal.statement.statement) as usize
                        ],
                    ),
                ))
                .await?;
            client.flush().await?;
            client.set_state(PgWireConnectionState::CopyInProgress);
            return Ok(());
        }
        if is_copy_to_stdout(&portal.statement.statement) {
            let session_id = session_id_from_client(client);
            self.registry
                .set_current_query(&session_id, &portal.statement.statement);
            self.registry.finish_current_query(&session_id);
            client
                .send(PgWireBackendMessage::CopyOutResponse(
                    pgwire::messages::copy::CopyOutResponse::new(
                        copy_format_code(&portal.statement.statement),
                        copy_column_count(&portal.statement.statement),
                        vec![
                            copy_format_code(&portal.statement.statement) as i16;
                            copy_column_count(&portal.statement.statement) as usize
                        ],
                    ),
                ))
                .await?;
            client
                .send(PgWireBackendMessage::CopyDone(CopyDone::new()))
                .await?;
            client
                .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                    "COPY 0".to_owned(),
                )))
                .await?;
            return Ok(());
        }

        let returning_statement = portal
            .statement
            .statement
            .to_ascii_uppercase()
            .contains("RETURNING");
        let pgjdbc_generated_key_returning =
            returning_statement && portal.statement.statement.contains('"');
        let should_send_execute_row_description = pgjdbc_generated_key_returning
            || (returning_statement
                && !client
                    .metadata()
                    .contains_key(&described_statement_key(&portal.statement.statement))
                && !client
                    .metadata()
                    .contains_key(&described_portal_key(portal_name)));

        match self.do_query(client, portal.as_ref(), max_rows).await? {
            Response::EmptyQuery => {
                client
                    .send(PgWireBackendMessage::EmptyQueryResponse(
                        EmptyQueryResponse::new(),
                    ))
                    .await?;
            }
            Response::Query(results) => {
                let fields = results.row_schema();
                let mut rows = Vec::new();
                let mut data_rows = results.data_rows();
                while let Some(row) = data_rows.next().await {
                    rows.push(row?);
                }
                if should_send_execute_row_description {
                    client
                        .send(PgWireBackendMessage::RowDescription(row_description(
                            &fields,
                        )))
                        .await?;
                }
                let mut cursor = PortalCursor {
                    fields,
                    rows,
                    position: 0,
                    total_rows: 0,
                };
                cursor.total_rows = cursor.rows.len();
                let (chunk, suspended) = cursor.next_chunk(max_rows);
                for row in chunk {
                    client.send(PgWireBackendMessage::DataRow(row)).await?;
                }
                if suspended {
                    self.cursors.write().unwrap().insert(key, cursor);
                    client
                        .send(PgWireBackendMessage::PortalSuspended(PortalSuspended::new()))
                        .await?;
                } else {
                    client
                        .send(PgWireBackendMessage::CommandComplete(
                            Tag::new("SELECT").with_rows(cursor.position).into(),
                        ))
                        .await?;
                }
            }
            Response::Execution(tag) => {
                let command: CommandComplete = tag.into();
                apply_command_tag_to_tx_status(client, &command.tag);
                client
                    .send(PgWireBackendMessage::CommandComplete(command))
                    .await?;
            }
            Response::Error(err) => {
                mark_error_status(client);
                client
                    .send(PgWireBackendMessage::ErrorResponse((*err).into()))
                    .await?;
            }
            Response::CopyIn(_) | Response::CopyOut(_) | Response::CopyBoth(_) => {
                return Err(user_error(
                    "ERROR",
                    "0A000",
                    "COPY response is only supported by COPY statements",
                ));
            }
        }
        Ok(())
    }

    async fn on_sync<C>(&self, client: &mut C, _message: PgSync) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                tx_status_from_client(client),
            )))
            .await?;
        client.flush().await?;
        client.set_state(PgWireConnectionState::ReadyForQuery);
        Ok(())
    }

    async fn on_close<C>(&self, client: &mut C, message: Close) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        match message.target_type {
            TARGET_TYPE_BYTE_STATEMENT => {
                client.portal_store().rm_statement(name);
            }
            TARGET_TYPE_BYTE_PORTAL => {
                client.portal_store().rm_portal(name);
                let key = cursor_key(&session_id_from_client(client), name);
                self.cursors.write().unwrap().remove(&key);
            }
            _ => {}
        }
        client
            .send(PgWireBackendMessage::CloseComplete(CloseComplete::new()))
            .await?;
        Ok(())
    }

    /// Overrides the default so a parameterized statement that returns no rows
    /// (INSERT/UPDATE/DELETE/DDL) answers `Describe` with `NoData` instead of
    /// an empty `RowDescription`. pgwire's `send_describe_response` only emits
    /// `NoData` when there are no parameters either, and an empty
    /// `RowDescription` makes pgjdbc treat the statement as result-returning,
    /// discarding its batch update counts.
    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        match message.target_type {
            TARGET_TYPE_BYTE_STATEMENT => {
                if let Some(stmt) = client.portal_store().get_statement(name) {
                    client
                        .metadata_mut()
                        .insert(described_statement_key(&stmt.statement), "1".to_owned());
                    let resp = self.do_describe_statement(client, &stmt).await?;
                    if resp.fields.is_empty() {
                        client
                            .send(PgWireBackendMessage::ParameterDescription(
                                ParameterDescription::new(
                                    resp.parameters.iter().map(|t| t.oid()).collect(),
                                ),
                            ))
                            .await?;
                        client
                            .send(PgWireBackendMessage::NoData(NoData::new()))
                            .await?;
                    } else {
                        client
                            .send(PgWireBackendMessage::ParameterDescription(
                                ParameterDescription::new(
                                    resp.parameters.iter().map(|t| t.oid()).collect(),
                                ),
                            ))
                            .await?;
                        let metadata = self.output_metadata_for_query(client, &stmt.statement);
                        let row_desc = if metadata.is_empty() {
                            row_description(&resp.fields)
                        } else {
                            row_description_from_metadata(&metadata, |_, _| FieldFormat::Text)
                        };
                        client
                            .send(PgWireBackendMessage::RowDescription(row_desc))
                            .await?;
                    }
                } else {
                    return Err(PgWireError::StatementNotFound(name.to_owned()));
                }
            }
            TARGET_TYPE_BYTE_PORTAL => {
                if let Some(portal) = client.portal_store().get_portal(name) {
                    client
                        .metadata_mut()
                        .insert(described_portal_key(name), "1".to_owned());
                    let resp = self.do_describe_portal(client, &portal).await?;
                    if resp.fields.is_empty() {
                        client
                            .send(PgWireBackendMessage::NoData(NoData::new()))
                            .await?;
                    } else {
                        let metadata =
                            self.output_metadata_for_query(client, &portal.statement.statement);
                        let row_desc = if metadata.is_empty() {
                            row_description(&resp.fields)
                        } else {
                            row_description_from_metadata(&metadata, |i, ty| {
                                effective_result_format(
                                    ty,
                                    portal.result_column_format.format_for(i),
                                )
                            })
                        };
                        client
                            .send(PgWireBackendMessage::RowDescription(row_desc))
                            .await?;
                    }
                } else {
                    return Err(PgWireError::PortalNotFound(name.to_owned()));
                }
            }
            _ => return Err(PgWireError::InvalidTargetType(message.target_type)),
        }
        Ok(())
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let raw_sql = &portal.statement.statement;
        info!("Received extended query: {}", raw_sql);
        self.metrics.queries_total.inc();

        let session_id = session_id_from_client(client);
        if self.registry.is_cancelled(&session_id) {
            self.metrics.query_errors_total.inc();
            return Err(user_error(
                "ERROR",
                "57P01",
                "session terminated by administrator",
            ));
        }
        if self.registry.is_query_cancelled(&session_id) {
            self.metrics.query_errors_total.inc();
            return Err(user_error(
                "ERROR",
                "57014",
                "canceling statement due to user request",
            ));
        }
        self.registry.set_current_query(&session_id, raw_sql);
        let _current_query = CurrentQueryGuard {
            registry: &self.registry,
            session_id: &session_id,
        };

        let _timer = QueryTimer {
            start: std::time::Instant::now(),
            sql: raw_sql,
            session_id: &session_id,
            metrics: &self.metrics,
            slow_log: &self.slow_log,
        };

        let principal_id = principal_id_from_client(client);

        let ctx = nodus_executor::ExecutionContext {
            session_id: session_id.clone(),
            principal_id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };

        // Extract parameters from the portal natively into Vec<nodus_executor::Value>
        let len = portal.parameter_len();
        let mut params = Vec::with_capacity(len);
        for i in 0..len {
            let param_type = portal
                .statement
                .parameter_types
                .get(i)
                .unwrap_or(&Type::UNKNOWN);

            let format = portal.parameter_format.format_for(i);

            let val = if portal.parameters.get(i).is_none_or(|p| p.is_none()) {
                nodus_executor::Value::Null
            } else if format == pgwire::api::results::FieldFormat::Text {
                let bytes = portal.parameters.get(i).unwrap().as_ref().unwrap();
                let s = String::from_utf8_lossy(bytes).into_owned();
                text_parameter_value(param_type, s)
            } else {
                match *param_type {
                    Type::BOOL => {
                        let v = portal.parameter::<bool>(i, param_type)?.unwrap_or_default();
                        nodus_executor::Value::Bool(v)
                    }
                    Type::INT2 => {
                        let v = portal.parameter::<i16>(i, param_type)?.unwrap_or_default();
                        nodus_executor::Value::Int(v as i64)
                    }
                    Type::INT4 => {
                        let v = portal.parameter::<i32>(i, param_type)?.unwrap_or_default();
                        nodus_executor::Value::Int(v as i64)
                    }
                    Type::INT8 => {
                        let v = portal.parameter::<i64>(i, param_type)?.unwrap_or_default();
                        nodus_executor::Value::Int(v)
                    }
                    Type::FLOAT4 => {
                        let v = portal.parameter::<f32>(i, param_type)?.unwrap_or_default();
                        nodus_executor::Value::Float(v as f64)
                    }
                    Type::FLOAT8 => {
                        let v = portal.parameter::<f64>(i, param_type)?.unwrap_or_default();
                        nodus_executor::Value::Float(v)
                    }
                    Type::NUMERIC => {
                        let bytes = portal.parameters.get(i).unwrap().as_ref().unwrap();
                        let s = String::from_utf8_lossy(bytes).into_owned();
                        text_parameter_value(param_type, s)
                    }
                    Type::OID => {
                        let v = portal.parameter::<u32>(i, param_type)?.unwrap_or_default();
                        nodus_executor::Value::Int(v as i64)
                    }
                    Type::TEXT | Type::VARCHAR => {
                        let v = portal
                            .parameter::<String>(i, param_type)?
                            .unwrap_or_default();
                        nodus_executor::Value::Text(v)
                    }
                    Type::NAME | Type::BPCHAR => {
                        let v = portal
                            .parameter::<String>(i, param_type)?
                            .unwrap_or_default();
                        nodus_executor::Value::Text(v)
                    }
                    Type::BYTEA => {
                        let v = portal
                            .parameter::<Vec<u8>>(i, param_type)?
                            .unwrap_or_default();
                        nodus_executor::Value::Text(format!("\\x{}", hex_encode(&v)))
                    }
                    Type::DATE => {
                        let v = portal
                            .parameter::<NaiveDate>(i, param_type)?
                            .map(|v| v.to_string())
                            .unwrap_or_default();
                        nodus_executor::Value::Text(v)
                    }
                    Type::TIME => {
                        let v = portal
                            .parameter::<NaiveTime>(i, param_type)?
                            .map(|v| v.format("%H:%M:%S%.f").to_string())
                            .unwrap_or_default();
                        nodus_executor::Value::Text(v)
                    }
                    Type::TIMESTAMP => {
                        let v = portal
                            .parameter::<NaiveDateTime>(i, param_type)?
                            .map(|v| v.format("%Y-%m-%d %H:%M:%S%.f").to_string())
                            .unwrap_or_default();
                        nodus_executor::Value::Text(v)
                    }
                    Type::TIMESTAMPTZ => {
                        let v = portal
                            .parameter::<DateTime<Utc>>(i, param_type)?
                            .map(|v| v.to_rfc3339())
                            .unwrap_or_default();
                        nodus_executor::Value::Text(v)
                    }
                    Type::UUID => {
                        let v = portal
                            .parameter::<Uuid>(i, param_type)?
                            .map(|v| v.to_string())
                            .unwrap_or_default();
                        nodus_executor::Value::Text(v)
                    }
                    Type::JSON | Type::JSONB => {
                        let v = portal
                            .parameter::<Json<serde_json::Value>>(i, param_type)?
                            .map(|v| v.0)
                            .unwrap_or(serde_json::Value::Null);
                        nodus_executor::Value::Jsonb(v)
                    }
                    Type::TEXT_ARRAY
                    | Type::VARCHAR_ARRAY
                    | Type::BPCHAR_ARRAY
                    | Type::NAME_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<String>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, nodus_executor::Value::Text)
                    }
                    Type::INT2_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<i16>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |n| nodus_executor::Value::Int(n as i64))
                    }
                    Type::INT4_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<i32>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |n| nodus_executor::Value::Int(n as i64))
                    }
                    Type::INT8_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<i64>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, nodus_executor::Value::Int)
                    }
                    Type::FLOAT4_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<f32>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |n| nodus_executor::Value::Float(n as f64))
                    }
                    Type::FLOAT8_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<f64>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, nodus_executor::Value::Float)
                    }
                    Type::BOOL_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<bool>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, nodus_executor::Value::Bool)
                    }
                    Type::UUID_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<Uuid>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |uuid| nodus_executor::Value::Text(uuid.to_string()))
                    }
                    Type::JSON_ARRAY | Type::JSONB_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<Json<serde_json::Value>>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |json| nodus_executor::Value::Jsonb(json.0))
                    }
                    Type::BYTEA_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<Vec<u8>>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |bytes| {
                            nodus_executor::Value::Text(format!("\\x{}", hex_encode(&bytes)))
                        })
                    }
                    Type::DATE_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<NaiveDate>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |date| nodus_executor::Value::Text(date.to_string()))
                    }
                    Type::TIME_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<NaiveTime>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |time| {
                            nodus_executor::Value::Text(time.format("%H:%M:%S%.f").to_string())
                        })
                    }
                    Type::TIMESTAMP_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<NaiveDateTime>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |ts| {
                            nodus_executor::Value::Text(
                                ts.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
                            )
                        })
                    }
                    Type::TIMESTAMPTZ_ARRAY => {
                        let v = portal
                            .parameter::<Vec<Option<DateTime<Utc>>>>(i, param_type)?
                            .unwrap_or_default();
                        values_to_array(v, |ts| nodus_executor::Value::Text(ts.to_rfc3339()))
                    }
                    _ => {
                        let v = portal
                            .parameter::<String>(i, &Type::TEXT)
                            .unwrap_or(Some("".to_string()))
                            .unwrap_or_default();
                        nodus_executor::Value::Text(v)
                    }
                }
            };
            params.push(val);
        }

        let query_str = raw_sql;
        remember_statement_timeout(client, query_str);
        if statement_would_timeout(client, query_str) {
            self.metrics.query_errors_total.inc();
            mark_error_status(client);
            return Err(user_error(
                "ERROR",
                "57014",
                "canceling statement due to statement timeout",
            ));
        }
        if is_set_default_statement(query_str) {
            return Ok(Response::Execution(Tag::new("SET")));
        }

        let stmt = match nodus_sql::parse_sql(query_str) {
            Ok(mut stmts) if !stmts.is_empty() => stmts.remove(0),
            Ok(_) => return Ok(Response::Execution(Tag::new("OK"))),
            Err(e) => {
                error!("Failed to parse SQL: {}", e);
                self.metrics.query_errors_total.inc();
                let err = ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    format!("Syntax error: {}", e),
                );
                return Err(PgWireError::UserError(Box::new(err)));
            }
        };
        let plan = match nodus_executor::plan_statement(&stmt, &params) {
            Ok(plan) => plan,
            Err(e) => {
                error!("Failed to plan SQL: {}", e);
                self.metrics.query_errors_total.inc();
                let err = ErrorInfo::new(
                    "ERROR".to_owned(),
                    "0A000".to_owned(),
                    format!("Unsupported feature: {}", e),
                );
                return Err(PgWireError::UserError(Box::new(err)));
            }
        };

        let out = match self.executor.execute_logical(&ctx, plan) {
            Ok(out) => out,
            Err(e) => {
                let err_str = e.to_string();
                let code = sqlstate_for_execution_error(&err_str);
                let err = ErrorInfo::new("ERROR".to_owned(), code.to_owned(), err_str);
                mark_error_status(client);
                return Err(PgWireError::UserError(Box::new(err)));
            }
        };

        if self.registry.is_query_cancelled(&session_id) {
            self.metrics.query_errors_total.inc();
            mark_error_status(client);
            return Err(user_error(
                "ERROR",
                "57014",
                "canceling statement due to user request",
            ));
        }

        if out.columns.is_empty() {
            apply_command_tag_to_tx_status(client, &out.tag);
            let tag = command_tag_from_output_tag(&out.tag);
            return Ok(Response::Execution(tag));
        }

        let field_info = field_info_for_output(&out.columns, &out.types, |i, _| {
            portal.result_column_format.format_for(i)
        });
        let mut data_rows = Vec::new();
        for row in &out.rows {
            data_rows
                .push(encode_row(&row.values, field_info.clone()).map_err(PgWireError::IoError));
        }
        let response = QueryResponse::new(field_info, stream::iter(data_rows));
        Ok(Response::Query(response))
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut param_types = stmt.parameter_types.clone();
        if param_types.is_empty() {
            let mut max_param = 0;
            let query = &stmt.statement;
            for i in 1..=100 {
                let placeholder = format!("${}", i);
                if query.contains(&placeholder) {
                    max_param = i;
                }
            }
            if max_param > 0 {
                param_types = vec![Type::UNKNOWN; max_param];
            }
        }

        let session_id = client
            .metadata()
            .get("nodus_session_id")
            .cloned()
            .unwrap_or_default();
        let principal_id = client
            .metadata()
            .get("nodus_principal_id")
            .and_then(|s| Uuid::parse_str(s).ok())
            .map(PrincipalId)
            .unwrap_or_default();
        let ctx = nodus_executor::ExecutionContext {
            session_id,
            principal_id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };

        let query_str = stmt.statement.as_str();

        let mut fields = vec![];
        if let Ok(mut stmts) = nodus_sql::parse_sql(query_str)
            && let Some(parsed) = stmts.pop()
            && let Ok(plan) = nodus_executor::plan_statement(&parsed, &[])
        {
            let mut plan_zero = plan.clone();
            let mut can_execute = false;
            if let nodus_executor::LogicalPlan::Select { ref mut limit, .. } = plan_zero {
                *limit = Some(0);
                can_execute = true;
            } else if let nodus_executor::LogicalPlan::Insert {
                ref mut values_list,
                ref returning,
                ..
            } = plan_zero
            {
                if !returning.is_empty() {
                    values_list.clear();
                    can_execute = true;
                }
            } else if let nodus_executor::LogicalPlan::ShowVariable { .. } = plan_zero {
                can_execute = true;
            } else if let nodus_executor::LogicalPlan::SelectLiteral { .. } = plan_zero {
                can_execute = true;
            }

            if can_execute && let Ok(out) = self.executor.execute_logical(&ctx, plan_zero) {
                for (col_name, col_type) in out.columns.into_iter().zip(out.types) {
                    fields.push(FieldInfo::new(
                        col_name,
                        None,
                        None,
                        map_type(&col_type),
                        FieldFormat::Text,
                    ));
                }
            }
        }

        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query_str = portal.statement.statement.as_str();

        let session_id = client
            .metadata()
            .get("nodus_session_id")
            .cloned()
            .unwrap_or_default();
        let principal_id = client
            .metadata()
            .get("nodus_principal_id")
            .and_then(|s| Uuid::parse_str(s).ok())
            .map(PrincipalId)
            .unwrap_or_default();
        let ctx = nodus_executor::ExecutionContext {
            session_id,
            principal_id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };

        let mut fields = vec![];
        if let Ok(mut stmts) = nodus_sql::parse_sql(query_str)
            && let Some(parsed) = stmts.pop()
            && let Ok(plan) = nodus_executor::plan_statement(&parsed, &[])
        {
            let mut plan_zero = plan.clone();
            let mut can_execute = false;
            if let nodus_executor::LogicalPlan::Select { ref mut limit, .. } = plan_zero {
                *limit = Some(0);
                can_execute = true;
            } else if let nodus_executor::LogicalPlan::Insert {
                ref mut values_list,
                ref returning,
                ..
            } = plan_zero
            {
                if !returning.is_empty() {
                    values_list.clear();
                    can_execute = true;
                }
            } else if let nodus_executor::LogicalPlan::ShowVariable { .. } = plan_zero {
                can_execute = true;
            } else if let nodus_executor::LogicalPlan::SelectLiteral { .. } = plan_zero {
                can_execute = true;
            }

            if can_execute && let Ok(out) = self.executor.execute_logical(&ctx, plan_zero) {
                for (col_name, col_type) in out.columns.into_iter().zip(out.types) {
                    fields.push(FieldInfo::new(
                        col_name,
                        None,
                        None,
                        map_type(&col_type),
                        FieldFormat::Text,
                    ));
                }
            }
        }

        Ok(DescribePortalResponse::new(fields))
    }
}

#[derive(Default)]
pub struct NodusCopyHandler {
    registry: Arc<SessionRegistry>,
}

#[async_trait]
impl CopyHandler for NodusCopyHandler {
    async fn on_copy_data<C>(&self, client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let rows = client
            .metadata()
            .get(METADATA_COPY_ROWS)
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        let chunk_rows = copy_data
            .data
            .iter()
            .filter(|b| **b == b'\n')
            .count()
            .max(1);
        client.metadata_mut().insert(
            METADATA_COPY_ROWS.to_owned(),
            rows.saturating_add(chunk_rows).to_string(),
        );
        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let rows = client
            .metadata_mut()
            .remove(METADATA_COPY_ROWS)
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        let extended_copy = client
            .metadata_mut()
            .remove(METADATA_COPY_EXTENDED)
            .as_deref()
            == Some("1");
        let session_id = session_id_from_client(client);
        self.registry.finish_current_query(&session_id);
        client
            .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                format!("COPY {rows}"),
            )))
            .await?;
        if !extended_copy {
            client
                .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                    tx_status_from_client(client),
                )))
                .await?;
        }
        client.flush().await?;
        if extended_copy {
            client.set_state(PgWireConnectionState::AwaitingSync);
        } else {
            client.set_state(PgWireConnectionState::ReadyForQuery);
        }
        Ok(())
    }

    async fn on_copy_fail<C>(&self, client: &mut C, fail: CopyFail) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client.metadata_mut().remove(METADATA_COPY_ROWS);
        client.metadata_mut().remove(METADATA_COPY_EXTENDED);
        let session_id = session_id_from_client(client);
        self.registry.finish_current_query(&session_id);
        mark_error_status(client);
        let msg = if fail.message.is_empty() {
            "COPY failed".to_owned()
        } else {
            fail.message
        };
        client
            .send(PgWireBackendMessage::ErrorResponse(
                ErrorInfo::new("ERROR".to_owned(), "57014".to_owned(), msg).into(),
            ))
            .await?;
        client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                tx_status_from_client(client),
            )))
            .await?;
        client.flush().await?;
        client.set_state(PgWireConnectionState::ReadyForQuery);
        Ok(())
    }
}

pub struct NodusPgWireServer {
    startup_handler: Arc<NodusStartupHandler>,
    executor: Arc<dyn nodus_executor::Executor>,
    metrics: nodus_monitoring::Metrics,
    registry: Arc<SessionRegistry>,
    slow_log: Arc<nodus_monitoring::SlowQueryLog>,
    extended_query_handler: Arc<NodusExtendedQueryHandler>,
    copy_handler: Arc<NodusCopyHandler>,
}

impl PgWireHandlerFactory for NodusPgWireServer {
    type StartupHandler = NodusStartupHandler;
    type SimpleQueryHandler = NodusQueryHandler;
    type ExtendedQueryHandler = NodusExtendedQueryHandler;
    type CopyHandler = NodusCopyHandler;

    // Called once per connection by `process_socket`, so each client gets its
    // own session registered in the shared registry.
    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        Arc::new(NodusQueryHandler {
            session_id: String::new(),
            session_state: nodus_sql::SessionState::default(),
            executor: self.executor.clone(),
            metrics: self.metrics.clone(),
            registry: self.registry.clone(),
            slow_log: self.slow_log.clone(),
        })
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.extended_query_handler.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.startup_handler.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        self.copy_handler.clone()
    }
}

enum StartupControl {
    Plain(TcpStream),
    Secure(Box<tokio_rustls::server::TlsStream<TcpStream>>),
    Closed,
}

async fn negotiate_startup_control(
    mut socket: TcpStream,
    tls: Option<Arc<tokio_rustls::TlsAcceptor>>,
    registry: Arc<SessionRegistry>,
) -> Result<StartupControl, IoError> {
    loop {
        let mut header = [0u8; STARTUP_PACKET_HEADER_LEN];
        loop {
            let read = socket.peek(&mut header).await?;
            if read == 0 {
                return Ok(StartupControl::Closed);
            }
            if read >= STARTUP_PACKET_HEADER_LEN {
                break;
            }
        }
        let magic = (&header[4..8]).get_i32();
        match magic {
            CANCEL_REQUEST_MAGIC => {
                let mut packet = [0u8; CANCEL_REQUEST_LEN];
                socket.read_exact(&mut packet).await?;
                let mut body = &packet[8..];
                let pid = body.get_i32();
                let secret = body.get_i32();
                let accepted = registry.cancel_backend_query(pid, secret);
                info!(
                    "Received cancel request for backend pid={} accepted={}",
                    pid, accepted
                );
                return Ok(StartupControl::Closed);
            }
            SSL_REQUEST_MAGIC => {
                let mut packet = [0u8; STARTUP_PACKET_HEADER_LEN];
                socket.read_exact(&mut packet).await?;
                if let Some(acceptor) = tls {
                    socket.write_all(&[SslResponse::BYTE_ACCEPT]).await?;
                    let tls_socket = acceptor.accept(socket).await?;
                    return Ok(StartupControl::Secure(Box::new(tls_socket)));
                }
                socket.write_all(&[SslResponse::BYTE_REFUSE]).await?;
            }
            GSSENC_REQUEST_MAGIC => {
                let mut packet = [0u8; STARTUP_PACKET_HEADER_LEN];
                socket.read_exact(&mut packet).await?;
                socket.write_all(&[SslResponse::BYTE_REFUSE]).await?;
            }
            _ => return Ok(StartupControl::Plain(socket)),
        }
    }
}

fn register_socket_session<S>(
    framed: &mut Framed<S, PgWireMessageServerCodec<String>>,
    registry: &SessionRegistry,
) -> String {
    let session_id = Uuid::new_v4().to_string();
    let secret_uuid = Uuid::new_v4();
    let secret = (secret_uuid.as_u128() & 0x7fff_ffff) as i32;
    let pid = std::process::id() as i32;
    let session = Session {
        session_id: session_id.clone(),
        principal_id: PrincipalId::new(),
        active_roles: vec![],
        database_id: None,
    };
    registry.register(&session);
    registry.register_backend_key(&session_id, pid, secret);
    framed
        .codec_mut()
        .client_info
        .metadata
        .insert(METADATA_NODUS_SESSION_ID.to_owned(), session_id.clone());
    framed
        .codec_mut()
        .client_info
        .metadata
        .insert(METADATA_BACKEND_PID.to_owned(), pid.to_string());
    framed
        .codec_mut()
        .client_info
        .metadata
        .insert(METADATA_BACKEND_SECRET.to_owned(), secret.to_string());
    framed
        .codec_mut()
        .client_info
        .metadata
        .insert(METADATA_TX_STATUS.to_owned(), "I".to_owned());
    session_id
}

async fn process_nodus_message<S>(
    message: pgwire::messages::PgWireFrontendMessage,
    socket: &mut Framed<S, PgWireMessageServerCodec<String>>,
    factory: Arc<NodusPgWireServer>,
) -> PgWireResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    match socket.codec().client_info.state() {
        PgWireConnectionState::AwaitingStartup
        | PgWireConnectionState::AuthenticationInProgress => {
            factory
                .startup_handler()
                .on_startup(socket, message)
                .await?;
        }
        PgWireConnectionState::AwaitingSync => {
            if let pgwire::messages::PgWireFrontendMessage::Sync(sync) = message {
                factory
                    .extended_query_handler()
                    .on_sync(socket, sync)
                    .await?;
                socket.set_state(PgWireConnectionState::ReadyForQuery);
            }
        }
        PgWireConnectionState::CopyInProgress => match message {
            pgwire::messages::PgWireFrontendMessage::CopyData(copy_data) => {
                factory
                    .copy_handler()
                    .on_copy_data(socket, copy_data)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::CopyDone(copy_done) => {
                factory
                    .copy_handler()
                    .on_copy_done(socket, copy_done)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::CopyFail(copy_fail) => {
                factory
                    .copy_handler()
                    .on_copy_fail(socket, copy_fail)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::Sync(_) => {}
            pgwire::messages::PgWireFrontendMessage::Flush(_) => {
                socket.flush().await?;
            }
            _ => {
                return Err(user_error(
                    "ERROR",
                    "08P01",
                    "only COPY data, done, or fail messages are valid during COPY",
                ));
            }
        },
        _ => match message {
            pgwire::messages::PgWireFrontendMessage::Query(query) => {
                factory
                    .simple_query_handler()
                    .on_query(socket, query)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::Parse(parse) => {
                factory
                    .extended_query_handler()
                    .on_parse(socket, parse)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::Bind(bind) => {
                factory
                    .extended_query_handler()
                    .on_bind(socket, bind)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::Execute(execute) => {
                factory
                    .extended_query_handler()
                    .on_execute(socket, execute)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::Describe(describe) => {
                factory
                    .extended_query_handler()
                    .on_describe(socket, describe)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::Sync(sync) => {
                factory
                    .extended_query_handler()
                    .on_sync(socket, sync)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::Close(close) => {
                factory
                    .extended_query_handler()
                    .on_close(socket, close)
                    .await?;
            }
            pgwire::messages::PgWireFrontendMessage::Flush(_) => {
                socket.flush().await?;
            }
            pgwire::messages::PgWireFrontendMessage::Terminate(_) => {}
            pgwire::messages::PgWireFrontendMessage::CopyData(_)
            | pgwire::messages::PgWireFrontendMessage::CopyDone(_)
            | pgwire::messages::PgWireFrontendMessage::CopyFail(_) => {
                return Err(user_error(
                    "ERROR",
                    "08P01",
                    "COPY message outside COPY mode",
                ));
            }
            _ => {}
        },
    }
    Ok(())
}

async fn process_nodus_error<S>(
    socket: &mut Framed<S, PgWireMessageServerCodec<String>>,
    error: PgWireError,
    wait_for_sync: bool,
) -> Result<(), IoError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    mark_error_status(socket);
    match error {
        PgWireError::UserError(error_info) => {
            socket
                .feed(PgWireBackendMessage::ErrorResponse((*error_info).into()))
                .await?;
        }
        PgWireError::ApiError(e) => {
            let error_info = ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), e.to_string());
            socket
                .feed(PgWireBackendMessage::ErrorResponse(error_info.into()))
                .await?;
        }
        other => {
            let error_info =
                ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), other.to_string());
            socket
                .send(PgWireBackendMessage::ErrorResponse(error_info.into()))
                .await?;
            return socket.close().await;
        }
    }

    if wait_for_sync {
        socket.set_state(PgWireConnectionState::AwaitingSync);
    } else {
        socket
            .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                tx_status_from_client(socket),
            )))
            .await?;
    }
    socket.flush().await?;
    Ok(())
}

async fn process_framed_nodus_socket<S>(
    mut socket: Framed<S, PgWireMessageServerCodec<String>>,
    factory: Arc<NodusPgWireServer>,
) -> Result<(), IoError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    let session_id = register_socket_session(&mut socket, &factory.registry);
    while let Some(msg) = socket.next().await {
        let msg = match msg {
            Ok(msg) => msg,
            Err(e) => {
                process_nodus_error(&mut socket, e, false).await?;
                continue;
            }
        };
        if matches!(msg, pgwire::messages::PgWireFrontendMessage::Terminate(_)) {
            break;
        }
        let is_extended_query = msg.is_extended_query();
        if let Err(e) = process_nodus_message(msg, &mut socket, factory.clone()).await {
            process_nodus_error(&mut socket, e, is_extended_query).await?;
        }
    }
    factory.registry.deregister(&session_id);
    Ok(())
}

async fn process_nodus_socket(
    tcp_socket: TcpStream,
    tls: Option<Arc<tokio_rustls::TlsAcceptor>>,
    factory: Arc<NodusPgWireServer>,
) -> Result<(), IoError> {
    let addr = tcp_socket.peer_addr()?;
    tcp_socket.set_nodelay(true)?;

    match negotiate_startup_control(tcp_socket, tls, factory.registry.clone()).await? {
        StartupControl::Plain(socket) => {
            let client_info = DefaultClient::new(addr, false);
            let framed = Framed::new(socket, PgWireMessageServerCodec::new(client_info));
            process_framed_nodus_socket(framed, factory).await
        }
        StartupControl::Secure(socket) => {
            let client_info = DefaultClient::new(addr, true);
            let framed = Framed::new(*socket, PgWireMessageServerCodec::new(client_info));
            process_framed_nodus_socket(framed, factory).await
        }
        StartupControl::Closed => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn start_pgwire_server(
    listener: TcpListener,
    executor: Arc<dyn nodus_executor::Executor>,
    metrics: nodus_monitoring::Metrics,
    registry: Arc<SessionRegistry>,
    authenticator: Arc<PasswordAuthenticator>,
    slow_log: Arc<nodus_monitoring::SlowQueryLog>,
    tls: Option<Arc<tokio_rustls::TlsAcceptor>>,
    mut shutdown: tokio::sync::watch::Receiver<()>,
) -> anyhow::Result<()> {
    let mut param_provider = DefaultServerParameterProvider::default();
    param_provider.server_version = "16.0".to_string();

    let startup_handler = Arc::new(NodusStartupHandler {
        authenticator,
        param_provider,
        registry: registry.clone(),
    });
    let factory = Arc::new(NodusPgWireServer {
        startup_handler,
        executor: executor.clone(),
        metrics: metrics.clone(),
        registry: registry.clone(),
        slow_log: slow_log.clone(),
        extended_query_handler: Arc::new(NodusExtendedQueryHandler {
            executor,
            metrics: metrics.clone(),
            slow_log,
            registry: registry.clone(),
            cursors: RwLock::new(HashMap::new()),
        }),
        copy_handler: Arc::new(NodusCopyHandler {
            registry: registry.clone(),
        }),
    });

    info!(
        "PGWire server listening on {} (tls: {})",
        listener.local_addr()?,
        tls.is_some()
    );

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                info!("PGWire server shutting down...");
                break;
            }
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((socket, _)) => {
                        let factory = factory.clone();
                        let metrics = metrics.clone();
                        let tls = tls.clone();
                        tokio::spawn(async move {
                            metrics.pgwire_connections_total.inc();
                            metrics.active_sessions.inc();
                            if let Err(e) = process_nodus_socket(socket, tls, factory).await {
                                error!("Socket error: {}", e);
                            }
                            metrics.active_sessions.dec();
                        });
                    }
                    Err(e) => {
                        error!("PGWire accept error: {}", e);
                    }
                }
            }
        }
    }
    Ok(())
}
