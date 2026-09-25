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
    Numeric(rust_decimal::Decimal),
}

/// Encodes a row for storage. A numeric is written as its decimal text, so
/// the stored row stays readable by binaries that predate [`Value::Numeric`].
pub(crate) fn encode_row(row: &[Value]) -> serde_json::Result<String> {
    let stored: Vec<Value> = row
        .iter()
        .map(|v| match v {
            Value::Numeric(d) => Value::Text(d.to_string()),
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
            Value::Float(f) => rust_decimal::Decimal::from_f64_retain(*f)
                .and_then(|d| parse_decimal(&(d.normalize().to_string()))),
            Value::Int(i) => Some(rust_decimal::Decimal::from(*i)),
            _ => None,
        };
        if let Some(d) = restored {
            *value = Value::Numeric(d);
        }
    }
}

/// Whether a declared type is `jsonb`.
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
    d: rust_decimal::Decimal,
    data_type: &str,
) -> Result<rust_decimal::Decimal, String> {
    let Some(args) = data_type
        .split_once('(')
        .and_then(|(_, rest)| rest.split_once(')'))
        .map(|(args, _)| args)
    else {
        return Ok(d);
    };
    let mut parts = args.split(',').map(|p| p.trim().parse::<u32>());
    let precision = match parts.next() {
        Some(Ok(p)) => p,
        _ => return Ok(d),
    };
    let scale = match parts.next() {
        Some(Ok(s)) => s,
        _ => 0,
    };
    // Exact decimals carry at most 28 fractional digits.
    let scale = scale.min(28);
    let mut rounded =
        d.round_dp_with_strategy(scale, rust_decimal::RoundingStrategy::MidpointAwayFromZero);
    rounded.rescale(scale);
    let integer_digits = rounded
        .trunc()
        .abs()
        .to_string()
        .trim_start_matches('0')
        .len() as u32;
    if integer_digits > precision.saturating_sub(scale) {
        return Err("numeric field overflow".to_string());
    }
    Ok(rounded)
}

/// Parses decimal text (`1.50`, `-3`, `1e3`), keeping its scale.
pub(crate) fn parse_decimal(text: &str) -> Option<rust_decimal::Decimal> {
    use std::str::FromStr;
    let t = text.trim();
    rust_decimal::Decimal::from_str(t)
        .ok()
        .or_else(|| rust_decimal::Decimal::from_scientific(t).ok())
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
    let t = data_type.to_uppercase();
    // `INTERVAL` contains "INT" but is textual — check it before the INT rule.
    if t.contains("INTERVAL") {
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
    match value {
        Value::Null => Value::Null,
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
        // Text into a numeric, boolean, or date/time column must parse as
        // that type (date/time text is stored in its canonical form).
        Value::Text(_)
            if column_type(data_type) != ColumnType::Text || temporal_type(data_type).is_some() =>
        {
            crate::planner::cast_value(value.clone(), data_type)
        }
        Value::Text(s) => coerce(s, ColumnType::Text),
        scalar => match column_type(data_type) {
            // A number into an integer or numeric column is cast, so it is
            // checked against the column's width, precision, and scale.
            ColumnType::Int | ColumnType::Numeric => {
                crate::planner::cast_value(scalar.clone(), data_type)
            }
            ColumnType::Float => match scalar {
                Value::Float(_) => scalar.clone(),
                Value::Int(n) => Value::Float(*n as f64),
                Value::Numeric(d) => Value::Float(decimal_to_f64(d)),
                _ => coerce(&render(scalar), ColumnType::Float),
            },
            ColumnType::Bool => match scalar {
                Value::Bool(_) => scalar.clone(),
                _ => coerce(&render(scalar), ColumnType::Bool),
            },
            // TEXT/VARCHAR and the catch-all: keep the scalar's representation
            // (a numeric becomes its text, as a text column holds text).
            ColumnType::Text => match scalar {
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

pub(crate) fn render(value: &Value) -> String {
    match value {
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
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
        Value::Null => String::new(),
    }
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

/// Parses an ISO date or timestamp text into a `NaiveDateTime` (a bare date
/// becomes midnight; a zoned timestamp is converted to UTC).
fn parse_naive_dt(s: &str) -> Option<chrono::NaiveDateTime> {
    parse_temporal(s).map(|t| t.utc())
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

    /// The instant in UTC (a value without a zone is taken as UTC).
    pub(crate) fn utc(&self) -> chrono::NaiveDateTime {
        self.local() - chrono::Duration::seconds(self.offset.unwrap_or(0))
    }
}

/// Parses ISO date/timestamp text: `YYYY-MM-DD`, optionally followed (after a
/// space or `T`) by `HH:MM[:SS[.fraction]]` and a zone (`Z`, `UTC`, `+HH`,
/// `+HH:MM`, `-HHMM`).
pub(crate) fn parse_temporal(text: &str) -> Option<ParsedTemporal> {
    let text = text.trim();
    let (date_text, rest) = match text.find([' ', 'T']) {
        Some(at) => (&text[..at], text[at + 1..].trim()),
        None => (text, ""),
    };
    let date = chrono::NaiveDate::parse_from_str(date_text, "%Y-%m-%d").ok()?;
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
    let time = parse_time_of_day(&rest[..zone_at])?;
    let offset = parse_utc_offset(rest[zone_at..].trim())?;
    Some(ParsedTemporal {
        date,
        time: Some(time),
        offset,
    })
}

/// `HH:MM[:SS[.fraction]]`.
fn parse_time_of_day(text: &str) -> Option<chrono::NaiveTime> {
    let text = text.trim();
    chrono::NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
        .or_else(|_| chrono::NaiveTime::parse_from_str(text, "%H:%M"))
        .ok()
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
        let time =
            parse_time_of_day(trimmed).or_else(|| parse_temporal(trimmed).and_then(|t| t.time))?;
        return Some(
            format_timestamp(chrono::NaiveDate::default().and_time(time), false)[11..].to_string(),
        );
    }
    let parsed = parse_temporal(trimmed)?;
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

/// `date_trunc(unit, ts)` — truncates to year/month/day/hour/minute/second and
/// returns a timestamp text (PostgreSQL semantics).
pub(crate) fn date_trunc_text(unit: &str, ts: &str) -> Value {
    use chrono::{Datelike, NaiveDate, Timelike};
    let Some(dt) = parse_naive_dt(ts) else {
        return Value::Null;
    };
    let d = dt.date();
    let out = match unit.to_ascii_lowercase().as_str() {
        "year" => NaiveDate::from_ymd_opt(d.year(), 1, 1).and_then(|x| x.and_hms_opt(0, 0, 0)),
        "month" => {
            NaiveDate::from_ymd_opt(d.year(), d.month(), 1).and_then(|x| x.and_hms_opt(0, 0, 0))
        }
        "day" => d.and_hms_opt(0, 0, 0),
        "hour" => d.and_hms_opt(dt.hour(), 0, 0),
        "minute" => d.and_hms_opt(dt.hour(), dt.minute(), 0),
        "second" => d.and_hms_opt(dt.hour(), dt.minute(), dt.second()),
        _ => return Value::Null,
    };
    out.map(|x| Value::Text(x.format("%Y-%m-%d %H:%M:%S").to_string()))
        .unwrap_or(Value::Null)
}

fn days_in_month(year: i32, month: u32) -> i64 {
    let (ny, nm) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    match (
        chrono::NaiveDate::from_ymd_opt(year, month, 1),
        chrono::NaiveDate::from_ymd_opt(ny, nm, 1),
    ) {
        (Some(a), Some(b)) => (b - a).num_days(),
        _ => 30,
    }
}

/// `age(a, b)` — the calendar interval `a - b`, decomposed into years/months/
/// days with PostgreSQL-style borrowing, rendered as e.g. `1 year 2 mons`.
pub(crate) fn age_text(a: &str, b: &str) -> Value {
    use chrono::Datelike;
    let (Some(da), Some(db)) = (
        parse_naive_dt(a).map(|x| x.date()),
        parse_naive_dt(b).map(|x| x.date()),
    ) else {
        return Value::Null;
    };
    let mut years = da.year() - db.year();
    let mut months = da.month() as i32 - db.month() as i32;
    let mut days = da.day() as i32 - db.day() as i32;
    if days < 0 {
        let (py, pm) = if da.month() == 1 {
            (da.year() - 1, 12)
        } else {
            (da.year(), da.month() - 1)
        };
        days += days_in_month(py, pm) as i32;
        months -= 1;
    }
    if months < 0 {
        months += 12;
        years -= 1;
    }
    let mut parts = Vec::new();
    let unit = |n: i32, s: &str| format!("{n} {s}{}", if n.abs() == 1 { "" } else { "s" });
    if years != 0 {
        parts.push(unit(years, "year"));
    }
    if months != 0 {
        parts.push(unit(months, "mon"));
    }
    if days != 0 {
        parts.push(unit(days, "day"));
    }
    if parts.is_empty() {
        return Value::Text("00:00:00".to_string());
    }
    Value::Text(parts.join(" "))
}

/// A fixed ordering rank per value category, so cross-category comparisons are
/// total and deterministic instead of rendering to text. `Int`/`Float` share a
/// rank because they compare numerically.
fn type_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) | Value::Float(_) | Value::Numeric(_) => 2,
        Value::Text(_) => 3,
        Value::Array(_) => 4,
        Value::Jsonb(_) => 5,
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
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::Int(x), Value::Float(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::Float(x), Value::Int(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
        (Value::Numeric(x), Value::Numeric(y)) => x.cmp(y),
        (Value::Numeric(x), Value::Int(y)) => x.cmp(&rust_decimal::Decimal::from(*y)),
        (Value::Int(x), Value::Numeric(y)) => rust_decimal::Decimal::from(*x).cmp(y),
        (Value::Numeric(x), Value::Float(y)) => {
            decimal_to_f64(x).partial_cmp(y).unwrap_or(Ordering::Equal)
        }
        (Value::Float(x), Value::Numeric(y)) => {
            x.partial_cmp(&decimal_to_f64(y)).unwrap_or(Ordering::Equal)
        }
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
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
        // Different categories: order by rank, never by rendered text.
        _ => type_rank(a).cmp(&type_rank(b)),
    }
}

/// A decimal as the nearest float.
pub(crate) fn decimal_to_f64(d: &rust_decimal::Decimal) -> f64 {
    use rust_decimal::prelude::ToPrimitive;
    d.to_f64().unwrap_or(f64::NAN)
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
