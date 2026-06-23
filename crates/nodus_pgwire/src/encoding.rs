//! Value codec between `nodus_executor::Value` and the PostgreSQL wire format:
//! text rendering, binary (`ToSql`) encoding, array encoding, hex helpers, and
//! the typed parsers used when binding parameters and emitting `DataRow`s.

use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use pgwire::api::Type;
use pgwire::api::results::{FieldFormat, FieldInfo};
use pgwire::messages::data::DataRow;
use postgres_types::{IsNull, Json, Kind, ToSql};
use uuid::Uuid;

pub(crate) fn render_scalar_text(value: &nodus_executor::Value, declared: &Type) -> String {
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

pub(crate) fn render_bytea_text(raw: &str) -> String {
    if raw.starts_with("\\x") {
        raw.to_owned()
    } else {
        format!("\\x{}", hex_encode(raw.as_bytes()))
    }
}

pub(crate) fn render_array_text(arr: &[nodus_executor::Value]) -> String {
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

pub(crate) fn quote_array_value(value: &str) -> String {
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

pub(crate) fn value_to_string(value: &nodus_executor::Value) -> String {
    render_scalar_text(value, &Type::TEXT)
}

pub(crate) fn parse_i64(value: &nodus_executor::Value) -> std::io::Result<i64> {
    match value {
        nodus_executor::Value::Int(i) => Ok(*i),
        nodus_executor::Value::Float(f) => Ok(*f as i64),
        _ => value_to_string(value)
            .parse::<i64>()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
    }
}

pub(crate) fn parse_f64(value: &nodus_executor::Value) -> std::io::Result<f64> {
    match value {
        nodus_executor::Value::Int(i) => Ok(*i as f64),
        nodus_executor::Value::Float(f) => Ok(*f),
        _ => value_to_string(value)
            .parse::<f64>()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
    }
}

pub(crate) fn parse_bool(value: &nodus_executor::Value) -> std::io::Result<bool> {
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

pub(crate) fn parse_json(value: &nodus_executor::Value) -> std::io::Result<serde_json::Value> {
    match value {
        nodus_executor::Value::Jsonb(j) => Ok(j.clone()),
        _ => serde_json::from_str(&value_to_string(value))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
    }
}

pub(crate) fn parse_uuid(value: &nodus_executor::Value) -> std::io::Result<Uuid> {
    Uuid::parse_str(&value_to_string(value))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

pub(crate) fn parse_date(value: &nodus_executor::Value) -> std::io::Result<NaiveDate> {
    NaiveDate::parse_from_str(&value_to_string(value), "%Y-%m-%d")
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

pub(crate) fn parse_time(value: &nodus_executor::Value) -> std::io::Result<NaiveTime> {
    let s = value_to_string(value);
    NaiveTime::parse_from_str(&s, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(&s, "%H:%M"))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

pub(crate) fn parse_timestamp(value: &nodus_executor::Value) -> std::io::Result<NaiveDateTime> {
    let s = value_to_string(value).replace('T', " ");
    NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f")
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

pub(crate) fn parse_timestamptz(value: &nodus_executor::Value) -> std::io::Result<DateTime<Utc>> {
    let s = value_to_string(value);
    if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
        return Ok(dt.with_timezone(&Utc));
    }
    if let Ok(dt) = DateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f%#z") {
        return Ok(dt.with_timezone(&Utc));
    }
    parse_timestamp(value).map(|naive| naive.and_utc())
}

pub(crate) fn parse_bytea(value: &nodus_executor::Value) -> std::io::Result<Vec<u8>> {
    let raw = value_to_string(value);
    let Some(hex) = raw.strip_prefix("\\x").or_else(|| raw.strip_prefix("\\X")) else {
        return Ok(raw.into_bytes());
    };
    hex_decode(hex)
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(TABLE[(byte >> 4) as usize] as char);
        out.push(TABLE[(byte & 0x0f) as usize] as char);
    }
    out
}

pub(crate) fn hex_decode(hex: &str) -> std::io::Result<Vec<u8>> {
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

pub(crate) fn hex_value(byte: u8) -> std::io::Result<u8> {
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

pub(crate) fn append_tosql<T: ToSql>(
    row: &mut BytesMut,
    ty: &Type,
    value: &T,
) -> std::io::Result<()> {
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

pub(crate) fn append_text(row: &mut BytesMut, text: String) {
    row.put_i32(text.len() as i32);
    row.put_slice(text.as_bytes());
}

pub(crate) fn array_values(
    value: &nodus_executor::Value,
) -> std::io::Result<&[nodus_executor::Value]> {
    match value {
        nodus_executor::Value::Array(values) => Ok(values),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "expected array value",
        )),
    }
}

pub(crate) fn encode_array_binary(
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
        Type::TEXT_ARRAY
        | Type::VARCHAR_ARRAY
        | Type::BPCHAR_ARRAY
        | Type::NAME_ARRAY
        | Type::CHAR_ARRAY => append_tosql(
            row,
            declared,
            &values.iter().map(optional_string).collect::<Vec<_>>(),
        ),
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

pub(crate) fn optional_string(value: &nodus_executor::Value) -> Option<String> {
    if matches!(value, nodus_executor::Value::Null) {
        None
    } else {
        Some(value_to_string(value))
    }
}

pub(crate) fn optional_bool(value: &nodus_executor::Value) -> std::io::Result<Option<bool>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_bool(value).map(Some)
    }
}

pub(crate) fn optional_bytea(value: &nodus_executor::Value) -> std::io::Result<Option<Vec<u8>>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_bytea(value).map(Some)
    }
}

pub(crate) fn optional_date(value: &nodus_executor::Value) -> std::io::Result<Option<NaiveDate>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_date(value).map(Some)
    }
}

pub(crate) fn optional_time(value: &nodus_executor::Value) -> std::io::Result<Option<NaiveTime>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_time(value).map(Some)
    }
}

pub(crate) fn optional_timestamp(
    value: &nodus_executor::Value,
) -> std::io::Result<Option<NaiveDateTime>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_timestamp(value).map(Some)
    }
}

pub(crate) fn optional_timestamptz(
    value: &nodus_executor::Value,
) -> std::io::Result<Option<DateTime<Utc>>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_timestamptz(value).map(Some)
    }
}

pub(crate) fn optional_uuid(value: &nodus_executor::Value) -> std::io::Result<Option<Uuid>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_uuid(value).map(Some)
    }
}

pub(crate) fn optional_json(
    value: &nodus_executor::Value,
) -> std::io::Result<Option<Json<serde_json::Value>>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_json(value).map(Json).map(Some)
    }
}

pub(crate) fn optional_i16(value: &nodus_executor::Value) -> std::io::Result<Option<i16>> {
    optional_i64(value).map(|v| v.map(|n| n as i16))
}

pub(crate) fn optional_i32(value: &nodus_executor::Value) -> std::io::Result<Option<i32>> {
    optional_i64(value).map(|v| v.map(|n| n as i32))
}

pub(crate) fn optional_i64(value: &nodus_executor::Value) -> std::io::Result<Option<i64>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_i64(value).map(Some)
    }
}

pub(crate) fn optional_f32(value: &nodus_executor::Value) -> std::io::Result<Option<f32>> {
    optional_f64(value).map(|v| v.map(|n| n as f32))
}

pub(crate) fn optional_f64(value: &nodus_executor::Value) -> std::io::Result<Option<f64>> {
    if matches!(value, nodus_executor::Value::Null) {
        Ok(None)
    } else {
        parse_f64(value).map(Some)
    }
}

pub(crate) fn append_value(
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
        Type::CHAR | Type::NAME | Type::TEXT | Type::VARCHAR | Type::BPCHAR => {
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

pub(crate) fn encode_row(
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

pub(crate) fn parse_text_array_parameter(raw: &str) -> nodus_executor::Value {
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

pub(crate) fn text_parameter_value(param_type: &Type, raw: String) -> nodus_executor::Value {
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

pub(crate) fn values_to_array<T, F>(values: Vec<Option<T>>, render: F) -> nodus_executor::Value
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
