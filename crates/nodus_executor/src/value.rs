//! Value representation, type coercion, comparison, and scalar-function
//! evaluation — the data primitives shared across the planner and executor.

use serde::{Deserialize, Serialize};

/// A column definition parsed from `CREATE TABLE`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub unique: bool,
    pub primary: bool,
    /// Lowered `DEFAULT` expression, if declared. Defaulted so plans
    /// serialized before this field decode.
    #[serde(default)]
    pub default: Option<crate::plan_types::ScalarExpr>,
    /// For a `serial` or identity column: the sequence to create for it
    /// (named `<table>_<column>_seq`), which its default draws from.
    #[serde(default)]
    pub sequence: Option<crate::sequences::SequenceSpec>,
}

/// A typed cell value. Rows are stored as `Vec<Value>` in table-column order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Array(Vec<Value>),
    Jsonb(serde_json::Value),
    Null,
    /// An exact decimal (`numeric`/`decimal`), keeping its scale (`1.10`).
    /// Appended so older encodings decode. Rows store it as its decimal text
    /// (see [`encode_row`]), which every reader decodes; [`restore_row`] turns
    /// it back into a number from the column's type.
    Numeric(crate::numeric::Numeric),
    /// A `json` value: its text exactly as written or built, which
    /// PostgreSQL keeps (whitespace, key order, and duplicate keys). Rows
    /// store it as text, as they did before; [`restore_row`] turns a `json`
    /// column's text back into it.
    Json(String),
    /// A row value (`ROW(...)`, or a whole-row reference to a relation) as
    /// its fields' names and values, for the JSON functions that take one.
    /// Rows store it as its text.
    Record(Vec<(String, Value)>),
    /// A `bytea` value. Rows store it as its `\x` hex text, which
    /// [`restore_row`] turns back into bytes from the column's type.
    Bytea(Vec<u8>),
}

/// Encodes a row for storage. A numeric is written as its decimal text, so
/// the stored row stays readable by binaries that predate [`Value::Numeric`].
pub(crate) fn encode_row(row: &[Value]) -> serde_json::Result<String> {
    let stored: Vec<Value> = row
        .iter()
        .map(|v| match v {
            Value::Numeric(d) => Value::Text(d.to_string()),
            Value::Json(text) => Value::Text(text.clone()),
            Value::Record(_) => Value::Text(render(v)),
            Value::Bytea(bytes) => Value::Text(crate::bytea::hex_text(bytes)),
            other => other.clone(),
        })
        .collect();
    serde_json::to_string(&stored)
}

/// Restores the values in a decoded row from its columns' declared types: a
/// `numeric` column holds decimal text (or a float, if written before exact
/// decimals), and a `jsonb` column text written before documents were stored
/// parsed.
pub(crate) fn restore_row(row: &mut [Value], columns: &[nodus_catalog::ColumnDescriptor]) {
    for (value, column) in row.iter_mut().zip(columns) {
        // A composite column's text is its record.
        if let Value::Text(t) = &*value
            && let Some(record) = crate::user_types::stored_record(&column.data_type, t)
        {
            *value = record;
            continue;
        }
        if column.data_type.trim().eq_ignore_ascii_case("json") {
            if let Value::Text(t) = &*value {
                *value = Value::Json(t.clone());
            }
            continue;
        }
        if is_bytea_type(&column.data_type) {
            if let Value::Text(t) = &*value {
                *value = Value::Bytea(crate::bytea::from_stored(t));
            }
            continue;
        }
        if is_jsonb_type(&column.data_type) {
            if let Value::Text(t) = &*value
                && let Ok(json) = crate::json_text::parse(t)
            {
                *value = Value::Jsonb(json);
            }
            continue;
        }
        if column_type(&column.data_type) != ColumnType::Numeric {
            continue;
        }
        let restored = match &*value {
            Value::Text(t) => parse_decimal(t),
            Value::Float(f) => crate::numeric::Numeric::from_f64_exact(*f).map(|d| d.normalize()),
            Value::Int(i) => Some(crate::numeric::Numeric::from(*i)),
            _ => None,
        };
        if let Some(d) = restored {
            *value = Value::Numeric(d);
        }
    }
}

/// The object-identifier type (`regclass`, `regtype`, `regnamespace`) a
/// declared type names, upper-cased without its schema.
pub(crate) fn object_identifier_type(data_type: &str) -> Option<&'static str> {
    let upper = data_type.trim().to_ascii_uppercase();
    match upper.strip_prefix("PG_CATALOG.").unwrap_or(&upper) {
        "REGCLASS" => Some("REGCLASS"),
        "REGTYPE" => Some("REGTYPE"),
        "REGNAMESPACE" => Some("REGNAMESPACE"),
        _ => None,
    }
}

/// Whether a declared type is `jsonb`.
/// Whether a declared type is `bytea`.
pub(crate) fn is_bytea_type(data_type: &str) -> bool {
    data_type.trim().eq_ignore_ascii_case("bytea")
}

pub(crate) fn is_jsonb_type(data_type: &str) -> bool {
    data_type.trim().eq_ignore_ascii_case("jsonb")
}

/// Whether a declared type is `json` or `jsonb`.
pub(crate) fn is_json_type(data_type: &str) -> bool {
    is_jsonb_type(data_type) || data_type.trim().eq_ignore_ascii_case("json")
}

/// The range of an integer type: `smallint`, `integer` (also `serial`), or
/// `bigint`.
pub(crate) fn integer_range(data_type: &str) -> (i64, i64) {
    let t = data_type.trim().to_ascii_uppercase();
    if t.contains("BIG") || t == "INT8" || t == "SERIAL8" {
        (i64::MIN, i64::MAX)
    } else if t.contains("SMALL") || t == "INT2" || t == "SERIAL2" {
        (i16::MIN as i64, i16::MAX as i64)
    } else if matches!(
        t.as_str(),
        "INT" | "INTEGER" | "INT4" | "SERIAL" | "SERIAL4"
    ) {
        (i32::MIN as i64, i32::MAX as i64)
    } else {
        (i64::MIN, i64::MAX)
    }
}

/// Applies a `numeric(p, s)` type modifier: rounds to scale `s` (half away
/// from zero) and rejects a value needing more than `p - s` integer digits.
/// A bare `numeric` keeps the value as it is.
pub(crate) fn apply_numeric_typmod(
    d: crate::numeric::Numeric,
    data_type: &str,
) -> Result<crate::numeric::Numeric, String> {
    let Some(args) = data_type
        .split_once('(')
        .and_then(|(_, rest)| rest.split_once(')'))
        .map(|(args, _)| args)
    else {
        return Ok(d);
    };
    let mut parts = args.split(',').map(|p| p.trim().parse::<i64>());
    let precision = match parts.next() {
        Some(Ok(p)) => p,
        _ => return Ok(d),
    };
    let scale = match parts.next() {
        Some(Ok(s)) => s,
        _ => 0,
    };
    d.apply_typmod(precision, scale)
}

/// Whether a declared type is `real` (`float4`, or `float(p)` with at most
/// 24 bits of precision).
pub(crate) fn is_real_type(data_type: &str) -> bool {
    let upper = data_type.trim().to_ascii_uppercase();
    match upper.as_str() {
        "REAL" | "FLOAT4" => true,
        _ => upper
            .strip_prefix("FLOAT(")
            .and_then(|rest| rest.strip_suffix(')'))
            .and_then(|p| p.trim().parse::<u32>().ok())
            .is_some_and(|p| (1..=24).contains(&p)),
    }
}

/// A double rounded to `real`, failing when it overflows or underflows.
pub(crate) fn to_real(x: f64) -> Result<f64, String> {
    let r = x as f32;
    if r.is_infinite() && x.is_finite() {
        return Err("value out of range: overflow".into());
    }
    if r == 0.0 && x != 0.0 {
        return Err("value out of range: underflow".into());
    }
    Ok(f64::from(r))
}

/// Parses `double precision` (or `real`) input text as PostgreSQL does:
/// `NaN`, `Infinity`, and `inf` are accepted; a value too large or too
/// small for the type is an error.
pub(crate) fn parse_float_text(text: &str, real: bool) -> Result<f64, String> {
    let type_name = if real { "real" } else { "double precision" };
    let t = text.trim_matches(|c: char| matches!(c, ' ' | '\t' | '\n' | '\r' | '\x0b' | '\x0c'));
    let x: f64 = t
        .parse()
        .map_err(|_| format!("invalid input syntax for type {type_name}: \"{text}\""))?;
    let out_of_range = || format!("\"{text}\" is out of range for type {type_name}");
    let spelled_infinite = t.to_ascii_lowercase().contains("inf");
    let nonzero_digits = t
        .split(['e', 'E'])
        .next()
        .is_some_and(|m| m.bytes().any(|b| (b'1'..=b'9').contains(&b)));
    let x = if real {
        let r = x as f32;
        if (r.is_infinite() && !spelled_infinite) || (r == 0.0 && nonzero_digits) {
            return Err(out_of_range());
        }
        f64::from(r)
    } else {
        x
    };
    if (x.is_infinite() && !spelled_infinite) || (x == 0.0 && nonzero_digits) {
        return Err(out_of_range());
    }
    Ok(x)
}

/// The length limit of a character type: `varchar(n)`, or `char(n)`
/// (blank-padded; `char` alone is `char(1)`).
pub(crate) fn character_limit(data_type: &str) -> Option<(usize, bool)> {
    let upper = data_type.trim().to_ascii_uppercase();
    let (base, length) = match upper.split_once('(') {
        Some((base, rest)) => (
            base.trim().to_string(),
            Some(rest.strip_suffix(')')?.trim().parse::<usize>().ok()?),
        ),
        None => (upper.clone(), None),
    };
    match (base.as_str(), length) {
        ("VARCHAR" | "CHARACTER VARYING", Some(n)) => Some((n, false)),
        ("CHAR" | "CHARACTER" | "BPCHAR", Some(n)) => Some((n, true)),
        ("CHAR" | "CHARACTER", None) => Some((1, true)),
        _ => None,
    }
}

/// A string fitted to a `varchar(n)` or `char(n)`: an explicit cast cuts it
/// to `n` characters; storing it may only drop blanks. A `char(n)` value
/// keeps no trailing blanks, which are its padding (shown on output).
/// `Err` names the type a string is too long for.
pub(crate) fn fit_character(text: &str, data_type: &str, explicit: bool) -> Result<String, String> {
    let Some((limit, padded)) = character_limit(data_type) else {
        return Ok(text.to_string());
    };
    let mut out = match text.char_indices().nth(limit) {
        Some((cut, _)) => {
            if !explicit && text[cut..].chars().any(|c| c != ' ') {
                let name = if padded {
                    format!("character({limit})")
                } else {
                    format!("character varying({limit})")
                };
                return Err(format!("value too long for type {name}"));
            }
            text[..cut].to_string()
        }
        None => text.to_string(),
    };
    if padded {
        out.truncate(out.trim_end_matches(' ').len());
    }
    Ok(out)
}

/// A `char(n)` value blank-padded to its length.
pub(crate) fn pad_character(text: &str, length: usize) -> String {
    let count = text.chars().count();
    if count >= length {
        text.to_string()
    } else {
        format!("{text}{}", " ".repeat(length - count))
    }
}

/// Parses integer input text as PostgreSQL does: surrounding whitespace, a
/// sign, decimal digits or a `0x`, `0o`, or `0b` prefix, and underscores
/// between digits (`1_000`). `None` if the text is not an integer; one too
/// large for any integer type comes back as `i128::MAX`.
pub(crate) fn parse_integer_text(text: &str) -> Option<i128> {
    let t = text.trim_matches(|c: char| matches!(c, ' ' | '\t' | '\n' | '\r' | '\x0b' | '\x0c'));
    let (negative, body) = match t.as_bytes().first() {
        Some(b'-') => (true, &t[1..]),
        Some(b'+') => (false, &t[1..]),
        _ => (false, t),
    };
    let (radix, digits) = match body.as_bytes() {
        [b'0', b'x' | b'X', ..] => (16, &body[2..]),
        [b'0', b'o' | b'O', ..] => (8, &body[2..]),
        [b'0', b'b' | b'B', ..] => (2, &body[2..]),
        _ => (10, body),
    };
    let digits = crate::numeric::ungrouped(digits, radix)?;
    let magnitude = i128::from_str_radix(&digits, radix).unwrap_or(i128::MAX);
    Some(if negative { -magnitude } else { magnitude })
}

/// Parses decimal text (`1.50`, `-3`, `1e3`, `NaN`), keeping its scale.
pub(crate) fn parse_decimal(text: &str) -> Option<crate::numeric::Numeric> {
    crate::numeric::Numeric::parse(text).ok()
}

/// A value in the form used for grouping and deduplication keys: numerically
/// equal numerics (`1.10`, `1.1`) share one form, as PostgreSQL treats them.
pub(crate) fn key_form(value: &Value) -> Value {
    match value {
        Value::Numeric(d) => Value::Numeric(d.normalize()),
        Value::Array(items) => Value::Array(items.iter().map(key_form).collect()),
        other => other.clone(),
    }
}

/// Logical column type derived from a SQL type name.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ColumnType {
    Int,
    Float,
    /// `numeric` / `decimal`: exact decimals.
    Numeric,
    Bool,
    Text,
}

pub(crate) fn column_type(data_type: &str) -> ColumnType {
    // An enum's values are its labels; a domain's are its base type's.
    if let Some(t) = crate::user_types::lookup(data_type) {
        return match t.domain() {
            Some(domain) => column_type(&domain.base),
            None => ColumnType::Text,
        };
    }
    let t = data_type.to_uppercase();
    // `INTERVAL` contains "INT" but is textual — check it before the INT
    // rule, as the range, multirange, and network families are (`int4range`
    // and `int4multirange` contain "INT" too).
    if t.contains("INTERVAL")
        || crate::ranges::is_range_type(data_type)
        || crate::multiranges::is_multirange_type(data_type)
        || crate::net::is_net_type(data_type)
        || crate::geometric::is_geometric_type(data_type)
    {
        ColumnType::Text
    } else if t.contains("INT") || t.contains("SERIAL") || matches!(t.trim(), "OID" | "XID") {
        ColumnType::Int
    } else if t.contains("NUMERIC") || t.contains("DECIMAL") {
        ColumnType::Numeric
    } else if t.contains("FLOAT") || t.contains("DOUBLE") || t.contains("REAL") {
        ColumnType::Float
    } else if t.contains("BOOL") {
        ColumnType::Bool
    } else {
        ColumnType::Text
    }
}

/// Coerces a literal string into a typed value for the given column type.
/// Empty text is preserved; unparseable numerics become `Null`.
pub(crate) fn coerce(raw: &str, ty: ColumnType) -> Value {
    match ty {
        ColumnType::Int => raw.parse::<i64>().map(Value::Int).unwrap_or(Value::Null),
        ColumnType::Float => raw.parse::<f64>().map(Value::Float).unwrap_or(Value::Null),
        ColumnType::Numeric => parse_decimal(raw).map_or(Value::Null, Value::Numeric),
        ColumnType::Bool => raw.parse::<bool>().map(Value::Bool).unwrap_or(Value::Null),
        ColumnType::Text => Value::Text(raw.to_string()),
    }
}

/// Coerces a write value to its declared column type so a column never stores a
/// representation that disagrees with its type. `Text` is parsed into the column
/// type (as before). A non-`Text` value bound into a *scalar* column (INT, FLOAT,
/// BOOL) is normalized into that type — e.g. a `Float(3.7)` into an INT column
/// becomes `Int(4)` instead of being stored verbatim. Text-classified columns
/// also cover JSONB/ARRAY/UUID/timestamps, whose values must be preserved as-is,
/// so non-`Text` values bound into them are left untouched.
pub(crate) fn coerce_for_column(value: &Value, data_type: &str) -> Value {
    // A user type's value must be one of it; so must an array's elements.
    if let Some(t) = crate::user_types::lookup(data_type) {
        return crate::user_types::coerce(&t, value, false)
            .unwrap_or_else(crate::eval_error::raise);
    }
    if let Some(element) = array_element_type(data_type)
        && let Some(t) = crate::user_types::lookup(element)
    {
        let items = match value {
            Value::Array(items) => items.clone(),
            Value::Text(s) => match coerce_array_text(s, "TEXT[]") {
                Some(Value::Array(items)) => items,
                _ => return crate::eval_error::raise(format!("malformed array literal: \"{s}\"")),
            },
            other => return other.clone(),
        };
        fn each(t: &crate::user_types::UserType, items: Vec<Value>) -> Value {
            Value::Array(
                items
                    .into_iter()
                    .map(|item| match item {
                        Value::Array(inner) => each(t, inner),
                        item => crate::user_types::coerce(t, &item, false)
                            .unwrap_or_else(crate::eval_error::raise),
                    })
                    .collect(),
            )
        }
        return each(&t, items);
    }
    match value {
        Value::Null => Value::Null,
        // A `json` value is its text anywhere but a JSON column.
        Value::Json(text) if !is_json_type(data_type) => {
            coerce_for_column(&Value::Text(text.clone()), data_type)
        }
        Value::Json(text) if is_jsonb_type(data_type) => {
            crate::planner::cast_value(Value::Text(text.clone()), data_type)
        }
        Value::Record(_) => coerce_for_column(&Value::Text(render(value)), data_type),
        // Text into a bit string column must fit its length.
        Value::Text(s) if crate::bits::bit_type(data_type).is_some() => {
            match crate::bits::parse(s).and_then(|bits| crate::bits::fit(&bits, data_type, false)) {
                Ok(bits) => Value::Text(bits),
                Err(e) => crate::eval_error::raise(e),
            }
        }
        // Text into a `bytea` column is `bytea` input; bytes stay bytes.
        Value::Text(_) if is_bytea_type(data_type) => {
            crate::planner::cast_value(value.clone(), data_type)
        }
        Value::Bytea(_) if is_bytea_type(data_type) => value.clone(),
        Value::Bytea(bytes) => {
            coerce_for_column(&Value::Text(crate::bytea::hex_text(bytes)), data_type)
        }
        // JSON text must parse; a `jsonb` column stores the parsed document.
        Value::Text(_) if is_json_type(data_type) => {
            crate::planner::cast_value(value.clone(), data_type)
        }
        // Complex values are never re-typed: they belong to JSONB/ARRAY columns,
        // which the coarse `column_type` may misclassify (e.g. `INT[]` contains
        // "INT"), so coercing them would corrupt the value.
        Value::Array(_) | Value::Jsonb(_) => value.clone(),
        // Array text (`'{1,2,3}'`) into an array column becomes a typed array.
        // Malformed text is kept verbatim rather than silently nulled.
        Value::Text(s) if array_element_type(data_type).is_some() => {
            coerce_array_text(s, data_type).unwrap_or_else(|| value.clone())
        }
        // Text into a numeric, boolean, date/time, interval, or range column
        // must parse as that type (text is stored in its canonical form).
        Value::Text(_)
            if column_type(data_type) != ColumnType::Text
                || crate::datetime::Kind::of_type(data_type).is_some()
                || crate::ranges::is_range_type(data_type)
                || crate::multiranges::is_multirange_type(data_type)
                || crate::net::is_net_type(data_type)
                || crate::geometric::is_geometric_type(data_type) =>
        {
            crate::planner::cast_value(value.clone(), data_type)
        }
        Value::Text(s) => match fit_character(s, data_type, false) {
            Ok(fitted) => Value::Text(fitted),
            Err(e) => crate::eval_error::raise(e),
        },
        scalar => match column_type(data_type) {
            // A number into an integer or numeric column is cast, so it is
            // checked against the column's width, precision, and scale.
            ColumnType::Int | ColumnType::Numeric => {
                crate::planner::cast_value(scalar.clone(), data_type)
            }
            ColumnType::Float => match scalar {
                Value::Float(_) | Value::Int(_) | Value::Numeric(_) => {
                    crate::planner::cast_value(scalar.clone(), data_type)
                }
                _ => coerce(&render(scalar), ColumnType::Float),
            },
            ColumnType::Bool => match scalar {
                Value::Bool(_) => scalar.clone(),
                _ => coerce(&render(scalar), ColumnType::Bool),
            },
            // TEXT/VARCHAR and the catch-all: keep the scalar's representation
            // (a numeric becomes its text, as a text column holds text).
            ColumnType::Text => match scalar {
                Value::Numeric(_) | Value::Int(_) | Value::Float(_) | Value::Bool(_)
                    if character_limit(data_type).is_some() =>
                {
                    let text = match scalar {
                        Value::Bool(b) => b.to_string(),
                        other => render(other),
                    };
                    coerce_for_column(&Value::Text(text), data_type)
                }
                Value::Numeric(d) => Value::Text(d.to_string()),
                _ => scalar.clone(),
            },
        },
    }
}

/// PostgreSQL's name for a declared type, as used in error messages.
pub(crate) fn sql_type_name(data_type: &str) -> String {
    let t = data_type.trim().to_ascii_uppercase();
    let name = if t.contains("BIGINT") || t == "INT8" || t.contains("BIGSERIAL") {
        "bigint"
    } else if t.contains("SMALLINT") || t == "INT2" || t.contains("SMALLSERIAL") {
        "smallint"
    } else if t.contains("INT") || t.contains("SERIAL") {
        "integer"
    } else if t.contains("REAL") || t == "FLOAT4" {
        "real"
    } else if t.contains("DOUBLE") || t.contains("FLOAT") {
        "double precision"
    } else if t.contains("NUMERIC") || t.contains("DECIMAL") {
        "numeric"
    } else if t.contains("BOOL") {
        "boolean"
    } else if temporal_type(&t) == Some(Temporal::TimestampTz) {
        "timestamp with time zone"
    } else {
        return data_type.trim().to_ascii_lowercase();
    };
    name.to_string()
}

/// A value's SQL type category, for error messages.
pub(crate) fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Int(_) => "integer",
        Value::Float(_) => "double precision",
        Value::Numeric(_) => "numeric",
        Value::Text(_) => "text",
        Value::Bool(_) => "boolean",
        Value::Array(_) => "array",
        Value::Jsonb(_) => "jsonb",
        Value::Json(_) => "json",
        Value::Record(_) => "record",
        Value::Bytea(_) => "bytea",
        Value::Null => "unknown",
    }
}

/// Element type of an array type name (`INT[]`, `text[][]`), or `None` for a
/// scalar type.
pub(crate) fn array_element_type(data_type: &str) -> Option<&str> {
    let base = data_type.trim().strip_suffix("[]")?;
    Some(base.trim_end_matches("[]").trim_end())
}

/// Parses PostgreSQL's array text format (`{1,2,NULL}`, `{"a b",c}`,
/// `{{1,2},{3,4}}`, optionally prefixed by bounds such as `[0:1]=`). An
/// unquoted `NULL` is SQL NULL; quoted and backslash-escaped characters are
/// literal. Elements stay `Text` for the caller to type. `None` if malformed.
pub(crate) fn parse_array_literal(text: &str) -> Option<Vec<Value>> {
    let chars: Vec<char> = text.trim().chars().collect();
    let mut pos = 0;
    if chars.first() == Some(&'[') {
        pos = chars.iter().position(|&c| c == '=')? + 1;
    }
    let items = parse_array_items(&chars, &mut pos)?;
    chars[pos..]
        .iter()
        .all(|c| c.is_whitespace())
        .then_some(items)
}

fn parse_array_items(chars: &[char], pos: &mut usize) -> Option<Vec<Value>> {
    let skip_ws = |pos: &mut usize| {
        while chars.get(*pos).is_some_and(|c| c.is_whitespace()) {
            *pos += 1;
        }
    };
    skip_ws(pos);
    if chars.get(*pos) != Some(&'{') {
        return None;
    }
    *pos += 1;
    let mut items = Vec::new();
    skip_ws(pos);
    if chars.get(*pos) == Some(&'}') {
        *pos += 1;
        return Some(items);
    }
    loop {
        skip_ws(pos);
        match chars.get(*pos)? {
            '{' => items.push(Value::Array(parse_array_items(chars, pos)?)),
            '"' => {
                *pos += 1;
                let mut item = String::new();
                loop {
                    match chars.get(*pos)? {
                        '"' => break,
                        '\\' => {
                            *pos += 1;
                            item.push(*chars.get(*pos)?);
                        }
                        c => item.push(*c),
                    }
                    *pos += 1;
                }
                *pos += 1;
                items.push(Value::Text(item));
            }
            _ => {
                let mut item = String::new();
                while let Some(&c) = chars.get(*pos) {
                    match c {
                        ',' | '}' => break,
                        '\\' => {
                            *pos += 1;
                            item.push(*chars.get(*pos)?);
                        }
                        c => item.push(c),
                    }
                    *pos += 1;
                }
                let item = item.trim();
                if item.is_empty() {
                    return None;
                }
                items.push(if item.eq_ignore_ascii_case("NULL") {
                    Value::Null
                } else {
                    Value::Text(item.to_string())
                });
            }
        }
        skip_ws(pos);
        match chars.get(*pos)? {
            ',' => *pos += 1,
            '}' => {
                *pos += 1;
                return Some(items);
            }
            _ => return None,
        }
    }
}

/// Converts array text to an array typed by `array_type`'s element type.
/// `None` if the text is malformed or an element does not parse.
pub(crate) fn coerce_array_text(text: &str, array_type: &str) -> Option<Value> {
    let element_type = array_element_type(array_type)?;
    fn typed(items: Vec<Value>, element_type: &str) -> Option<Vec<Value>> {
        items
            .into_iter()
            .map(|item| match item {
                Value::Array(inner) => typed(inner, element_type).map(Value::Array),
                Value::Text(t) => crate::planner::try_cast(Value::Text(t), element_type).ok(),
                other => Some(other),
            })
            .collect()
    }
    typed(parse_array_literal(text)?, element_type).map(Value::Array)
}

/// A `double precision` as PostgreSQL writes it: the shortest digits that
/// read back exactly, in exponent form below 1e-4 or from 1e15, and
/// `Infinity`, `-Infinity`, and `NaN` spelled out.
pub fn float_text(f: f64) -> String {
    shortest_float_text(f, format!("{f:e}"), f.to_string(), 15)
}

/// A `real` as PostgreSQL writes it: as [`float_text`], in exponent form
/// from 1e6.
pub fn float4_text(f: f32) -> String {
    shortest_float_text(f as f64, format!("{f:e}"), f.to_string(), 6)
}

fn shortest_float_text(f: f64, scientific: String, fixed: String, digits: i32) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f < 0.0 { "-Infinity" } else { "Infinity" }.to_string();
    }
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    if f != 0.0 && !(-4..digits).contains(&exponent) {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{mantissa}e{sign}{:02}", exponent.abs())
    } else {
        fixed
    }
}

/// A value as text, as PostgreSQL writes it.
pub fn render(value: &Value) -> String {
    match value {
        Value::Int(n) => n.to_string(),
        Value::Float(f) => float_text(*f),
        Value::Numeric(d) => d.to_string(),
        Value::Text(s) => s.clone(),
        Value::Bool(b) => {
            if *b {
                "t".to_string()
            } else {
                "f".to_string()
            }
        }
        Value::Array(a) => {
            let rendered: Vec<String> = a.iter().map(render).collect();
            format!("{{{}}}", rendered.join(","))
        }
        Value::Jsonb(j) => crate::json_text::jsonb_text(j),
        Value::Json(text) => text.clone(),
        Value::Record(fields) => record_text(fields),
        Value::Bytea(bytes) => crate::bytea::hex_text(bytes),
        Value::Null => String::new(),
    }
}

/// A row value as PostgreSQL writes one: `(1,"a b",,"")`, a field quoted
/// when it is empty or holds a quote, backslash, parenthesis, comma, or
/// space, and a NULL field empty.
fn record_text(fields: &[(String, Value)]) -> String {
    let mut out = String::from("(");
    for (i, (_, value)) in fields.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        if *value == Value::Null {
            continue;
        }
        let text = render(value);
        let quoted = text.is_empty()
            || text
                .chars()
                .any(|c| matches!(c, '"' | '\\' | '(' | ')' | ',') || c.is_whitespace());
        if quoted {
            out.push('"');
            for c in text.chars() {
                if matches!(c, '"' | '\\') {
                    out.push(c);
                }
                out.push(c);
            }
            out.push('"');
        } else {
            out.push_str(&text);
        }
    }
    out.push(')');
    out
}

/// Encodes a literal projection-function argument back into the string form the
/// planner stores: `'text'` for strings, plain digits for numbers/bools. (The
/// projection model stores args as strings; [`resolve_scalar_arg`] parses them
/// back at evaluation time.)
pub(crate) fn literal_arg(value: &Value) -> String {
    match value {
        Value::Text(s) => format!("'{s}'"),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

/// Resolves one scalar-function argument to a value for a given row: a quoted
/// `'…'` literal, a numeric literal, or otherwise a column reference.
pub(crate) fn resolve_scalar_arg(arg: &str, row: &[Value], col_names: &[String]) -> Value {
    if arg.len() >= 2 && arg.starts_with('\'') && arg.ends_with('\'') {
        Value::Text(arg[1..arg.len() - 1].to_string())
    } else if let Ok(i) = arg.parse::<i64>() {
        Value::Int(i)
    } else if let Ok(f) = arg.parse::<f64>() {
        Value::Float(f)
    } else if let Some(i) = col_names
        .iter()
        .position(|tc| tc == arg || tc.ends_with(&format!(".{arg}")))
    {
        row.get(i).cloned().unwrap_or(Value::Null)
    } else if let Some((base, op, key)) = crate::filter_eval::parse_json_ref(arg) {
        // A JSON access (`col->>'k'`) encoded as a synthetic arg name.
        col_names
            .iter()
            .position(|tc| tc == &base || tc.ends_with(&format!(".{base}")))
            .and_then(|i| row.get(i))
            .map(|v| crate::filter_eval::json_extract(v, &op, &key))
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    }
}

/// Evaluates a built-in scalar function for the legacy string-argument
/// projection path. A name the library does not implement yields NULL here;
/// the planner and executor reject such calls outside catalog introspection.
pub(crate) fn eval_scalar_function(name: &str, args: &[Value]) -> Value {
    if crate::functions::is_known(name) {
        crate::functions::call(name, args)
    } else {
        Value::Null
    }
}

/// The `pg_*_is_visible(oid)` catalog functions.
pub(crate) fn is_visibility_fn(name: &str) -> bool {
    matches!(
        name,
        "PG_TABLE_IS_VISIBLE"
            | "PG_TYPE_IS_VISIBLE"
            | "PG_FUNCTION_IS_VISIBLE"
            | "PG_OPERATOR_IS_VISIBLE"
            | "PG_OPCLASS_IS_VISIBLE"
            | "PG_OPFAMILY_IS_VISIBLE"
            | "PG_COLLATION_IS_VISIBLE"
            | "PG_CONVERSION_IS_VISIBLE"
            | "PG_STATISTICS_OBJ_IS_VISIBLE"
            | "PG_TS_CONFIG_IS_VISIBLE"
            | "PG_TS_DICT_IS_VISIBLE"
            | "PG_TS_PARSER_IS_VISIBLE"
            | "PG_TS_TEMPLATE_IS_VISIBLE"
    )
}

/// The date/time types, which NodusDB stores as ISO text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Temporal {
    Date,
    Time,
    Timestamp,
    TimestampTz,
}

/// The date/time type a declared type names, if any (`TIMESTAMP(3) WITH TIME
/// ZONE` is `TimestampTz`).
pub(crate) fn temporal_type(data_type: &str) -> Option<Temporal> {
    let upper = data_type.trim().to_ascii_uppercase();
    let without_precision = match (upper.find('('), upper.find(')')) {
        (Some(open), Some(close)) if open < close => {
            format!("{}{}", &upper[..open], &upper[close + 1..])
        }
        _ => upper,
    };
    let words = without_precision
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    Some(match words.as_str() {
        "DATE" => Temporal::Date,
        "TIME" | "TIME WITHOUT TIME ZONE" => Temporal::Time,
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => Temporal::Timestamp,
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => Temporal::TimestampTz,
        _ => return None,
    })
}

/// A parsed date/time literal: a date, an optional time of day, and an
/// optional UTC offset in seconds.
pub(crate) struct ParsedTemporal {
    pub(crate) date: chrono::NaiveDate,
    pub(crate) time: Option<chrono::NaiveTime>,
    pub(crate) offset: Option<i64>,
}

impl ParsedTemporal {
    fn local(&self) -> chrono::NaiveDateTime {
        self.date.and_time(self.time.unwrap_or_default())
    }

    /// The instant in UTC (a value without a zone is local time in the
    /// session's zone).
    pub(crate) fn utc(&self) -> chrono::NaiveDateTime {
        match self.offset {
            Some(offset) => self.local() - chrono::Duration::seconds(offset),
            None => crate::timezone::from_session_local(self.local()),
        }
    }
}

/// Parses ISO date/timestamp text: `YYYY-MM-DD`, optionally followed (after a
/// space or `T`) by `HH:MM[:SS[.fraction]]` and a zone (`Z`, `UTC`, `+HH`,
/// `+HH:MM`, `-HHMM`, a zone name).
pub(crate) fn parse_temporal(text: &str) -> Option<ParsedTemporal> {
    let text = text.trim();
    let (date_text, rest) = match text.find([' ', 'T']) {
        Some(at) => (&text[..at], text[at + 1..].trim()),
        None => (text, ""),
    };
    let date = chrono::NaiveDate::parse_from_str(date_text, "%Y-%m-%d").ok()?;
    with_time_of_day(date, rest)
}

/// `date` with the time of day and zone in `rest`, either of which may be
/// absent; `24:00:00` is the next day's midnight.
fn with_time_of_day(date: chrono::NaiveDate, rest: &str) -> Option<ParsedTemporal> {
    if rest.is_empty() {
        return Some(ParsedTemporal {
            date,
            time: None,
            offset: None,
        });
    }
    let zone_at = rest
        .find(|c: char| c == '+' || c == '-' || c == ' ' || c.is_ascii_alphabetic())
        .unwrap_or(rest.len());
    let mut micros = parse_time_of_day(&rest[..zone_at])?;
    let mut date = date;
    if micros >= crate::datetime::MICROS_PER_DAY {
        date = date.succ_opt()?;
        micros -= crate::datetime::MICROS_PER_DAY;
    }
    let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(
        (micros / 1_000_000) as u32,
        (micros % 1_000_000 * 1_000) as u32,
    )?;
    let zone = rest[zone_at..].trim();
    let offset = match parse_utc_offset(zone) {
        Some(offset) => offset,
        // A zone name's offset is the one in effect at that local time.
        None => Some(
            crate::timezone::Zone::resolve(zone)
                .ok()?
                .offset_at_local(date.and_time(time)),
        ),
    };
    Some(ParsedTemporal {
        date,
        time: Some(time),
        offset,
    })
}

/// Date input in the other forms PostgreSQL reads: a month name
/// (`Jan 5 2024`, `5 January 2024`, `January 5, 2024`), slashes
/// (`2024/01/05`, or `01/05/2024` as month/day/year), or run-together digits
/// (`20240105`); a time of day and zone may follow.
fn parse_loose_temporal(text: &str) -> Option<ParsedTemporal> {
    let words: Vec<&str> = text
        .split(|c: char| c.is_whitespace() || c == ',')
        .filter(|w| !w.is_empty())
        .collect();
    let is_time = |w: &str| w.contains(':');
    let date_words = words.iter().take_while(|w| !is_time(w)).count();
    let rest = words[date_words..].join(" ");
    let number = |w: &str| {
        w.bytes()
            .all(|b| b.is_ascii_digit())
            .then(|| w.parse::<i64>().ok())
            .flatten()
    };
    let month = |w: &str| {
        let lower = w.to_ascii_lowercase();
        (lower.len() >= 3)
            .then(|| {
                crate::datetime_format::MONTHS
                    .iter()
                    .position(|m| m.to_ascii_lowercase().starts_with(&lower))
            })
            .flatten()
            .map(|i| i as i64 + 1)
    };
    let (year, month, day) = match &words[..date_words] {
        [one] if one.contains('/') || one.contains('-') => {
            let parts: Vec<&str> = one.split(['/', '-']).collect();
            let [a, b, c] = parts[..] else { return None };
            match (number(a), month(b).or_else(|| number(b)), number(c)) {
                (Some(y), Some(m), Some(d)) if a.len() >= 3 => (y, m, d),
                (Some(m), Some(d), Some(y)) if !b.chars().any(|ch| ch.is_ascii_alphabetic()) => {
                    (y, m, d)
                }
                (Some(d), Some(m), Some(y)) => (y, m, d),
                _ => return None,
            }
        }
        [one] if one.len() == 8 => {
            let digits = number(one)?;
            (digits / 10_000, digits / 100 % 100, digits % 100)
        }
        // A month name and two numbers: the first is the year when it is
        // written with three or more digits, else the day.
        [a, b, c] => {
            let words = [*a, *b, *c];
            let m = words.iter().position(|w| month(w).is_some())?;
            let others: Vec<&str> = (0..3).filter(|i| *i != m).map(|i| words[i]).collect();
            let (first, second) = (number(others[0])?, number(others[1])?);
            if others[0].len() >= 3 {
                (first, month(words[m])?, second)
            } else {
                (second, month(words[m])?, first)
            }
        }
        _ => return None,
    };
    let date =
        chrono::NaiveDate::from_ymd_opt(i32::try_from(year).ok()?, month as u32, day as u32)?;
    with_time_of_day(date, &rest)
}

/// `HH:MM[:SS[.fraction]]` as microseconds since midnight, the fraction
/// rounded to microseconds; `24:00:00` is allowed, and a leap second
/// (`:60`) is the next minute.
fn parse_time_of_day(text: &str) -> Option<i64> {
    let text = text.trim();
    let mut parts = text.split(':');
    let field = |p: Option<&str>| {
        p.filter(|p| !p.is_empty() && p.len() <= 2 && p.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|p| p.parse::<i64>().ok())
    };
    let hours = field(parts.next())?;
    let minutes = field(parts.next())?;
    let seconds = match parts.next() {
        None => 0.0,
        Some(s) => {
            let (whole, fraction) = s.split_once('.').unwrap_or((s, ""));
            if !fraction.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let whole = field(Some(whole))? as f64;
            whole + format!("0.{fraction}0").parse::<f64>().ok()?
        }
    };
    if parts.next().is_some() || hours > 24 || minutes > 59 || seconds >= 61.0 {
        return None;
    }
    let micros = (hours * 3_600 + minutes * 60) * 1_000_000 + (seconds * 1e6).round() as i64;
    (micros <= crate::datetime::MICROS_PER_DAY).then_some(micros)
}

/// A zone suffix as seconds east of UTC: `None` when absent, and a parse
/// failure (`None` outer) when malformed.
fn parse_utc_offset(zone: &str) -> Option<Option<i64>> {
    if zone.is_empty() {
        return Some(None);
    }
    if ["Z", "UTC", "GMT"]
        .iter()
        .any(|z| zone.eq_ignore_ascii_case(z))
    {
        return Some(Some(0));
    }
    let sign = match zone.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let digits: String = zone[1..].chars().filter(|c| *c != ':').collect();
    if !digits.bytes().all(|b| b.is_ascii_digit()) || !matches!(digits.len(), 1 | 2 | 4) {
        return None;
    }
    let (hours, minutes) = if digits.len() == 4 {
        (
            digits[..2].parse::<i64>().ok()?,
            digits[2..].parse::<i64>().ok()?,
        )
    } else {
        (digits.parse::<i64>().ok()?, 0)
    };
    Some(Some(sign * (hours * 3600 + minutes * 60)))
}

/// Canonical PostgreSQL text for a date/time value of type `ty`, or `None` if
/// `text` is not valid input for it. A zoned timestamp is shown in UTC.
pub(crate) fn normalize_temporal(text: &str, ty: Temporal) -> Option<String> {
    let trimmed = text.trim();
    let lower = trimmed.to_ascii_lowercase();
    if ty != Temporal::Time && matches!(lower.as_str(), "infinity" | "-infinity") {
        return Some(lower);
    }
    if lower == "epoch" && ty != Temporal::Time {
        return normalize_temporal("1970-01-01 00:00:00+00", ty);
    }
    if ty == Temporal::Time {
        if lower == "allballs" {
            return Some("00:00:00".to_string());
        }
        let micros = parse_time_of_day(trimmed).or_else(|| {
            parse_temporal(trimmed)
                .and_then(|t| t.time)
                .map(|time| crate::datetime::time_micros_of(time))
        })?;
        return Some(crate::datetime::format_time(micros));
    }
    // Words for moments relative to now, in the session's zone.
    let now = || {
        let micros = crate::session_env::with(|e| e.map(|e| e.transaction_micros))
            .unwrap_or_else(crate::session_env::wall_micros);
        chrono::DateTime::from_timestamp_micros(micros)
            .map(|dt| crate::timezone::to_session_local(dt.naive_utc()))
    };
    let midnight = |days: i64| {
        now().and_then(|(local, _)| {
            Some(ParsedTemporal {
                date: local
                    .date()
                    .checked_add_signed(chrono::Duration::days(days))?,
                time: Some(chrono::NaiveTime::MIN),
                offset: None,
            })
        })
    };
    let relative = match lower.as_str() {
        "now" => now().map(|(local, offset)| ParsedTemporal {
            date: local.date(),
            time: Some(local.time()),
            offset: Some(offset),
        }),
        "today" => midnight(0),
        "tomorrow" => midnight(1),
        "yesterday" => midnight(-1),
        _ => None,
    };
    let parsed = match relative {
        Some(parsed) => parsed,
        None => parse_temporal(trimmed).or_else(|| parse_loose_temporal(trimmed))?,
    };
    Some(match ty {
        Temporal::Date => parsed.date.format("%Y-%m-%d").to_string(),
        // A timestamp without time zone ignores any zone in its input.
        Temporal::Timestamp => format_timestamp(parsed.local(), false),
        Temporal::TimestampTz => format_timestamp(parsed.utc(), true),
        Temporal::Time => unreachable!("handled above"),
    })
}

/// Whether text has the shape of a date or time (digits separated by `-` or
/// `:`), so a failed parse means an out-of-range field rather than bad syntax.
pub(crate) fn looks_temporal(text: &str) -> bool {
    let text = text.trim();
    let date = text.split([' ', 'T']).next().unwrap_or_default();
    let is_numeric_parts = |part: &str, sep: char, count: usize| {
        let parts: Vec<&str> = part.split(sep).collect();
        parts.len() == count
            && parts
                .iter()
                .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
    };
    is_numeric_parts(date, '-', 3)
        || is_numeric_parts(text, ':', 2)
        || is_numeric_parts(text.split('.').next().unwrap_or_default(), ':', 3)
}

/// The fractional-second precision a temporal type names (`TIMESTAMP(3)`).
fn fractional_precision(data_type: &str) -> Option<u32> {
    let open = data_type.find('(')?;
    let close = data_type[open..].find(')')? + open;
    data_type[open + 1..close].trim().parse().ok()
}

/// `text` (a canonical temporal of the type's kind) rounded to the precision
/// the type names: `'…00.1235'::timestamp(3)` is `…00.124`, and rounding may
/// carry into the next second, minute, day, or `24:00:00`.
pub(crate) fn apply_temporal_typmod(text: &str, data_type: &str) -> String {
    let Some(precision) = fractional_precision(data_type).map(|p| p.min(6)) else {
        return text.to_string();
    };
    if precision == 6 || matches!(text, "infinity" | "-infinity") {
        return text.to_string();
    }
    let step = 10i64.pow(6 - precision);
    // PostgreSQL rounds half away from zero relative to its 2000-01-01 epoch,
    // so a value before it rounds the other way on the wall clock.
    let round = |micros: i64| {
        if micros >= 0 {
            (micros + step / 2) / step * step
        } else {
            -((-micros + step / 2) / step * step)
        }
    };
    // Microseconds from 1970-01-01 to PostgreSQL's epoch.
    const EPOCH_2000: i64 = 946_684_800_000_000;
    match temporal_type(data_type) {
        Some(Temporal::Time) => crate::datetime::time_micros(text)
            .map(|micros| crate::datetime::format_time(round(micros)))
            .unwrap_or_else(|| text.to_string()),
        Some(Temporal::TimestampTz) => parse_temporal(text)
            .map(|parsed| {
                let micros = parsed.utc().and_utc().timestamp_micros() - EPOCH_2000;
                chrono::DateTime::from_timestamp_micros(round(micros) + EPOCH_2000)
                    .map(|dt| format_timestamp(dt.naive_utc(), true))
                    .unwrap_or_else(|| text.to_string())
            })
            .unwrap_or_else(|| text.to_string()),
        Some(Temporal::Timestamp) => parse_temporal(text)
            .map(|parsed| {
                let micros = parsed.local().and_utc().timestamp_micros() - EPOCH_2000;
                chrono::DateTime::from_timestamp_micros(round(micros) + EPOCH_2000)
                    .map(|dt| format_timestamp(dt.naive_utc(), false))
                    .unwrap_or_else(|| text.to_string())
            })
            .unwrap_or_else(|| text.to_string()),
        _ => text.to_string(),
    }
}

/// PostgreSQL's timestamp text: seconds always shown, a fraction only when
/// non-zero, and `+00` for a timestamp with time zone (shown in UTC).
pub(crate) fn format_timestamp(ts: chrono::NaiveDateTime, with_zone: bool) -> String {
    let mut out = ts.format("%Y-%m-%d %H:%M:%S").to_string();
    let micros = ts.and_utc().timestamp_subsec_micros();
    if micros != 0 {
        let frac = format!("{micros:06}");
        out.push('.');
        out.push_str(frac.trim_end_matches('0'));
    }
    if with_zone {
        out.push_str("+00");
    }
    out
}

/// A fixed ordering rank per value category, so cross-category comparisons are
/// total and deterministic instead of rendering to text. `Int`/`Float` share a
/// rank because they compare numerically.
fn type_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) | Value::Float(_) | Value::Numeric(_) => 2,
        Value::Text(_) | Value::Json(_) => 3,
        Value::Array(_) => 4,
        Value::Jsonb(_) => 5,
        Value::Record(_) => 6,
        Value::Bytea(_) => 7,
    }
}

/// Total ordering over values, used for ORDER BY, DISTINCT, MIN/MAX, and (via
/// [`values_equal`]) SQL equality so they all agree.
///
/// Numbers compare by magnitude (`Int`/`Float` interchangeably); within a
/// category the natural order applies. Values of *different* categories are
/// never compared by their rendered text — that made `Int(5)` and `Text("5")`
/// compare *equal* while `=` treated them as distinct, silently corrupting
/// `WHERE`/`JOIN`/`ORDER BY`/aggregates on any column holding mixed types.
/// Cross-category pairs order by [`type_rank`], keeping the relation total and
/// consistent with equality.
pub(crate) fn compare(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => float_cmp(*x, *y),
        (Value::Int(x), Value::Float(y)) => float_cmp(*x as f64, *y),
        (Value::Float(x), Value::Int(y)) => float_cmp(*x, *y as f64),
        (Value::Numeric(x), Value::Numeric(y)) => x.cmp(y),
        (Value::Numeric(x), Value::Int(y)) => x.cmp(&crate::numeric::Numeric::from(*y)),
        (Value::Int(x), Value::Numeric(y)) => crate::numeric::Numeric::from(*x).cmp(y),
        (Value::Numeric(x), Value::Float(y)) => float_cmp(decimal_to_f64(x), *y),
        (Value::Float(x), Value::Numeric(y)) => float_cmp(*x, decimal_to_f64(y)),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Text(x) | Value::Json(x), Value::Text(y) | Value::Json(y)) => x.cmp(y),
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Array(x), Value::Array(y)) => {
            for (xe, ye) in x.iter().zip(y.iter()) {
                let ord = compare(xe, ye);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            x.len().cmp(&y.len())
        }
        (Value::Jsonb(x), Value::Jsonb(y)) => crate::json_text::jsonb_cmp(x, y),
        (Value::Bytea(x), Value::Bytea(y)) => x.cmp(y),
        (Value::Record(x), Value::Record(y)) => {
            for ((_, xe), (_, ye)) in x.iter().zip(y.iter()) {
                let ord = compare(xe, ye);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            x.len().cmp(&y.len())
        }
        // Different categories: order by rank, never by rendered text.
        _ => type_rank(a).cmp(&type_rank(b)),
    }
}

/// A decimal as the nearest float.
pub(crate) fn decimal_to_f64(d: &crate::numeric::Numeric) -> f64 {
    d.to_f64()
}

/// Float order as PostgreSQL's: NaN equals itself and sorts above every
/// other value, and `-0` equals `0`.
pub(crate) fn float_cmp(x: f64, y: f64) -> std::cmp::Ordering {
    match (x.is_nan(), y.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        _ => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
    }
}

/// SQL value equality, defined as `compare(a, b) == Equal` so ordering and
/// equality never disagree. Numerically-equal `Int`/`Float` are equal; values of
/// different categories (e.g. `Int(5)` vs `Text("5")`) are not. NULL handling
/// (three-valued logic) is the caller's responsibility — callers that need it
/// short-circuit on NULL before calling this.
pub(crate) fn values_equal(a: &Value, b: &Value) -> bool {
    compare(a, b) == std::cmp::Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn floats_print_as_postgresql_prints_them() {
        assert_eq!(float_text(1.5), "1.5");
        assert_eq!(float_text(1e20), "1e+20");
        assert_eq!(float_text(1e-7), "1e-07");
        assert_eq!(float_text(1e14), "100000000000000");
        assert_eq!(float_text(0.0001), "0.0001");
        assert_eq!(float_text(f64::INFINITY), "Infinity");
        assert_eq!(float_text(f64::NEG_INFINITY), "-Infinity");
        assert_eq!(float_text(f64::NAN), "NaN");
        assert_eq!(float4_text(1e6), "1e+06");
        assert_eq!(float4_text(1234567.0), "1.234567e+06");
        assert_eq!(float4_text(100000.0), "100000");
    }

    #[test]
    fn temporal_typmods_round_half_away_from_the_2000_epoch() {
        let cast = |text: &str, ty: &str| apply_temporal_typmod(text, ty);
        assert_eq!(
            cast("2020-01-01 00:00:00.123456", "TIMESTAMP(3)"),
            "2020-01-01 00:00:00.123"
        );
        assert_eq!(
            cast("2020-01-01 00:00:00.1235", "TIMESTAMP(3)"),
            "2020-01-01 00:00:00.124"
        );
        assert_eq!(
            cast("2020-01-01 23:59:59.9999", "TIMESTAMP(3)"),
            "2020-01-02 00:00:00"
        );
        // Before PostgreSQL's epoch a tie rounds the other way on the clock.
        assert_eq!(
            cast("1999-01-01 00:00:00.1235", "TIMESTAMP(3)"),
            "1999-01-01 00:00:00.123"
        );
        assert_eq!(
            cast("1999-01-01 00:00:00.1245", "TIMESTAMP(3)"),
            "1999-01-01 00:00:00.124"
        );
        assert_eq!(
            cast("1969-12-31 23:59:59.1235+00", "TIMESTAMPTZ(3)"),
            "1969-12-31 23:59:59.123+00"
        );
        assert_eq!(
            cast("2020-01-01 00:00:00.123456+00", "TIMESTAMPTZ(3)"),
            "2020-01-01 00:00:00.123+00"
        );
        assert_eq!(cast("23:59:59.6", "TIME(0)"), "24:00:00");
        assert_eq!(cast("12:00:00.9999", "TIME(3)"), "12:00:01");
        // No precision, or one the text already has, leaves it alone.
        assert_eq!(
            cast("2020-01-01 00:00:00.123456", "TIMESTAMP"),
            "2020-01-01 00:00:00.123456"
        );
        assert_eq!(
            cast("2020-01-01 00:00:00.123456", "TIMESTAMP(6)"),
            "2020-01-01 00:00:00.123456"
        );
        assert_eq!(cast("infinity", "TIMESTAMP(3)"), "infinity");
    }

    #[test]
    fn date_input_takes_postgresql_forms() {
        let date = |s: &str| normalize_temporal(s, Temporal::Date);
        for text in [
            "Jan 5 2024",
            "5 January 2024",
            "January 5, 2024",
            "2024/01/05",
            "01/05/2024",
            "20240105",
        ] {
            assert_eq!(date(text).as_deref(), Some("2024-01-05"), "{text}");
        }
        assert_eq!(
            normalize_temporal("2024-01-01 10:00:00.1234567", Temporal::Timestamp).as_deref(),
            Some("2024-01-01 10:00:00.123457")
        );
        assert_eq!(
            normalize_temporal("2024-01-01 24:00:00", Temporal::Timestamp).as_deref(),
            Some("2024-01-02 00:00:00")
        );
        assert_eq!(
            normalize_temporal("23:59:60", Temporal::Time).as_deref(),
            Some("24:00:00")
        );
        assert_eq!(
            normalize_temporal("allballs", Temporal::Time).as_deref(),
            Some("00:00:00")
        );
        assert!(normalize_temporal("25:00", Temporal::Time).is_none());
        assert!(date("2024-02-30").is_none());
    }

    #[test]
    fn coerce_for_column_normalizes_scalars_but_preserves_complex_values() {
        use serde_json::json;
        // A non-Text value bound into a scalar column is normalized into it.
        assert_eq!(coerce_for_column(&Value::Float(3.7), "INT"), Value::Int(4));
        assert_eq!(
            coerce_for_column(&Value::Int(5), "DOUBLE"),
            Value::Float(5.0)
        );
        // Text still parses into the column type.
        assert_eq!(
            coerce_for_column(&Value::Text("7".into()), "INTEGER"),
            Value::Int(7)
        );
        // JSONB and ARRAY columns are Text-classified, but their non-Text values
        // must be preserved (not stringified).
        let j = Value::Jsonb(json!({"a": 1}));
        assert_eq!(coerce_for_column(&j, "JSONB"), j);
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        assert_eq!(coerce_for_column(&arr, "INT[]"), arr);
        assert_eq!(
            coerce_for_column(&Value::Text(String::new()), "TEXT"),
            Value::Text(String::new())
        );
        assert_eq!(coerce_for_column(&Value::Null, "TEXT"), Value::Null);
        // NULL is preserved.
        assert_eq!(coerce_for_column(&Value::Null, "INT"), Value::Null);
    }

    #[test]
    fn cross_type_orders_by_rank_not_rendered_text() {
        // The core soundness bug: `Int(5)` and `Text("5")` rendered to "5" and
        // compared *equal*. They are different categories and must not be equal.
        assert_eq!(
            compare(&Value::Int(5), &Value::Text("5".into())),
            Ordering::Less
        );
        assert!(!values_equal(&Value::Int(5), &Value::Text("5".into())));
        // And ordering is by category rank, not lexical text ("10" < "9").
        assert_eq!(
            compare(&Value::Int(10), &Value::Text("9".into())),
            Ordering::Less
        );
        // Bool vs Text, Null vs anything: total, deterministic, never equal.
        assert_eq!(
            compare(&Value::Bool(true), &Value::Text("t".into())),
            Ordering::Less
        );
        assert!(!values_equal(&Value::Bool(true), &Value::Text("t".into())));
        assert_eq!(compare(&Value::Null, &Value::Int(0)), Ordering::Less);
    }

    #[test]
    fn equality_agrees_with_ordering() {
        // For every pair, `values_equal` is exactly `compare == Equal`.
        let vals = [
            Value::Null,
            Value::Bool(false),
            Value::Bool(true),
            Value::Int(5),
            Value::Float(5.0),
            Value::Float(5.5),
            Value::Text("5".into()),
            Value::Text("abc".into()),
        ];
        for a in &vals {
            for b in &vals {
                assert_eq!(
                    values_equal(a, b),
                    compare(a, b) == Ordering::Equal,
                    "inconsistent for {a:?} vs {b:?}"
                );
            }
        }
        // Numerically-equal Int/Float are equal; same value across categories is not.
        assert!(values_equal(&Value::Int(5), &Value::Float(5.0)));
        assert!(!values_equal(&Value::Int(5), &Value::Bool(true)));
        assert!(values_equal(&Value::Null, &Value::Null));
    }

    #[test]
    fn mixed_int_float_compares_numerically_not_lexically() {
        // The lexical bug: "9" > "10". Numerically 9 < 10.
        assert_eq!(compare(&Value::Int(9), &Value::Float(10.0)), Ordering::Less);
        assert_eq!(
            compare(&Value::Float(10.0), &Value::Int(9)),
            Ordering::Greater
        );
        assert_eq!(compare(&Value::Int(2), &Value::Float(2.0)), Ordering::Equal);
        assert_eq!(
            compare(&Value::Float(2.5), &Value::Int(2)),
            Ordering::Greater
        );
        // Same-type paths are unaffected.
        assert_eq!(compare(&Value::Int(9), &Value::Int(10)), Ordering::Less);
    }
}
