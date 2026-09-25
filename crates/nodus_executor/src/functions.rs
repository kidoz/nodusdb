//! Built-in scalar functions.
//!
//! [`is_known`] is the single registry the planner consults: a call to any
//! other function is rejected rather than evaluated to a placeholder. Most
//! functions are strict — a NULL argument yields NULL without calling them —
//! except those PostgreSQL defines otherwise (see [`NON_STRICT`]). Domain and
//! input errors fail the statement through [`crate::eval_error`].

use crate::eval_error::raise;
use crate::session_env;
use crate::value::{Value, render, values_equal};

/// Functions that receive NULL arguments instead of short-circuiting to NULL.
const NON_STRICT: &[&str] = &[
    "__SLICE__",
    "ARRAY",
    "COALESCE",
    "NULLIF",
    "GREATEST",
    "LEAST",
    "CONCAT",
    "CONCAT_WS",
    "FORMAT",
    "NUM_NULLS",
    "NUM_NONNULLS",
    "QUOTE_NULLABLE",
    "JSON_BUILD_OBJECT",
    "JSONB_BUILD_OBJECT",
    "JSON_BUILD_ARRAY",
    "JSONB_BUILD_ARRAY",
    "TO_JSON",
    "TO_JSONB",
    "ARRAY_APPEND",
    "ARRAY_PREPEND",
    "ARRAY_CAT",
    "ARRAY_POSITION",
    "ARRAY_POSITIONS",
    "ARRAY_REMOVE",
    "ARRAY_REPLACE",
    "ARRAY_TO_STRING",
    "STRING_TO_ARRAY",
    "CURRENT_SETTING",
    "PG_TYPEOF",
    "FORMAT_TYPE",
];

/// Every function name (upper-cased, without a `pg_catalog.` qualifier) the
/// evaluator implements.
pub(crate) fn is_known(name: &str) -> bool {
    crate::value::is_visibility_fn(name)
        || matches!(
            name,
            // Strings.
            "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "OCTET_LENGTH" | "BIT_LENGTH"
                | "UPPER" | "LOWER" | "INITCAP" | "CASEFOLD" | "SUBSTR" | "SUBSTRING"
                | "STRPOS" | "OVERLAY" | "TRIM" | "BTRIM" | "LTRIM" | "RTRIM" | "LPAD"
                | "RPAD" | "REPLACE" | "TRANSLATE" | "REPEAT" | "REVERSE" | "SPLIT_PART" | "MD5"
                | "LEFT" | "RIGHT" | "CONCAT" | "CONCAT_WS" | "FORMAT" | "QUOTE_IDENT"
                | "QUOTE_LITERAL" | "QUOTE_NULLABLE" | "ASCII" | "CHR" | "TO_HEX" | "TO_BIN"
                | "TO_OCT" | "STARTS_WITH" | "REGEXP_REPLACE" | "REGEXP_MATCH" | "REGEXP_LIKE"
                | "REGEXP_COUNT" | "REGEXP_SUBSTR" | "REGEXP_SPLIT_TO_ARRAY"
                | "STRING_TO_ARRAY" | "ARRAY_TO_STRING"
                // Conditionals.
                | "ARRAY" | "COALESCE" | "NULLIF" | "GREATEST" | "LEAST" | "NUM_NULLS" | "NUM_NONNULLS"
                // Math.
                | "ABS" | "SIGN" | "CEIL" | "CEILING" | "FLOOR" | "ROUND" | "TRUNC" | "MOD"
                | "DIV" | "POWER" | "POW" | "SQRT" | "CBRT" | "EXP" | "LN" | "LOG" | "LOG10"
                | "PI" | "DEGREES" | "RADIANS" | "SIN" | "COS" | "TAN" | "COT" | "ASIN"
                | "ACOS" | "ATAN" | "ATAN2" | "SINH" | "COSH" | "TANH" | "GCD" | "LCM"
                | "FACTORIAL" | "RANDOM" | "WIDTH_BUCKET"
                // Dates and times.
                | "NOW" | "CURRENT_TIMESTAMP" | "TRANSACTION_TIMESTAMP"
                | "STATEMENT_TIMESTAMP" | "CLOCK_TIMESTAMP" | "CURRENT_DATE" | "CURRENT_TIME"
                | "LOCALTIMESTAMP" | "LOCALTIME" | "DATE_TRUNC" | "AGE" | "DATE_PART"
                | "MAKE_DATE" | "MAKE_TIMESTAMP" | "TO_TIMESTAMP"
                // Session and system.
                | "VERSION" | "CURRENT_USER" | "SESSION_USER" | "CURRENT_ROLE" | "USER"
                | "CURRENT_DATABASE" | "CURRENT_CATALOG" | "CURRENT_SCHEMA" | "CURRENT_SCHEMAS"
                | "CURRENT_SETTING" | "PG_BACKEND_PID" | "PG_TYPEOF" | "TXID_CURRENT"
                | "PG_CURRENT_XACT_ID" | "PG_SIZE_PRETTY" | "PG_ENCODING_TO_CHAR"
                | "PG_CLIENT_ENCODING" | "PG_IS_IN_RECOVERY" | "PG_SLEEP" | "PG_GET_USERBYID"
                | "INET_SERVER_ADDR" | "INET_SERVER_PORT" | "INET_CLIENT_ADDR"
                | "INET_CLIENT_PORT" | "OBJ_DESCRIPTION" | "COL_DESCRIPTION"
                | "SHOBJ_DESCRIPTION" | "FORMAT_TYPE" | "PG_GET_EXPR"
                | "PG_RELATION_IS_PUBLISHABLE" | "PG_GET_STATISTICSOBJDEF_COLUMNS"
                | "PG_GET_INDEXDEF" | "PG_GET_CONSTRAINTDEF" | "__OBJECT_NAME__"
                | "PG_GET_VIEWDEF"
                // Sequences.
                | "NEXTVAL" | "CURRVAL" | "LASTVAL" | "SETVAL" | "PG_GET_SERIAL_SEQUENCE"
                | "__IDENTITY__"
                // UUIDs.
                | "GEN_RANDOM_UUID" | "UUIDV4" | "UUIDV7" | "UUID_EXTRACT_VERSION"
                // JSON.
                | "TO_JSON" | "TO_JSONB" | "JSON_BUILD_OBJECT" | "JSONB_BUILD_OBJECT"
                | "JSON_BUILD_ARRAY" | "JSONB_BUILD_ARRAY" | "JSON_TYPEOF" | "JSONB_TYPEOF"
                | "JSON_ARRAY_LENGTH" | "JSONB_ARRAY_LENGTH" | "JSON_EXTRACT_PATH"
                | "JSONB_EXTRACT_PATH" | "JSON_EXTRACT_PATH_TEXT" | "JSONB_EXTRACT_PATH_TEXT"
                | "JSONB_SET" | "JSONB_STRIP_NULLS" | "JSON_STRIP_NULLS" | "JSONB_PRETTY"
                // Arrays.
                | "ARRAY_LENGTH" | "CARDINALITY" | "ARRAY_APPEND" | "ARRAY_PREPEND"
                | "ARRAY_CAT" | "ARRAY_POSITION" | "ARRAY_POSITIONS" | "ARRAY_REMOVE"
                | "ARRAY_REPLACE" | "ARRAY_UPPER" | "ARRAY_LOWER" | "ARRAY_NDIMS"
                | "TRIM_ARRAY" | "ARRAY_SORT" | "ARRAY_REVERSE"
                // Subscripts: `a[i]`, `a[lo:hi]`, `doc['key']`.
                | "__SUBSCRIPT__" | "__SLICE__"
                | crate::result_types::INTEGER_RANGE
        )
}

/// The declared result type of a call, for describing result columns before
/// any row exists; `None` when it depends on more than the argument types
/// NodusDB tracks. `arg_types` are the arguments' types where known.
pub(crate) fn return_type(name: &str, arg_types: &[Option<String>]) -> Option<String> {
    let first_known = || arg_types.iter().flatten().next().cloned();
    // An element of an array is of its element type; a slice of the array's.
    let subscripted = arg_types.first().cloned().flatten();
    match name {
        crate::result_types::INTEGER_RANGE => return subscripted,
        "__SUBSCRIPT__" => {
            return subscripted.map(|t| t.strip_suffix("[]").map(str::to_string).unwrap_or(t));
        }
        "__SLICE__" => return subscripted,
        _ => {}
    }
    Some(
        match name {
            "LENGTH"
            | "CHAR_LENGTH"
            | "CHARACTER_LENGTH"
            | "OCTET_LENGTH"
            | "BIT_LENGTH"
            | "STRPOS"
            | "ASCII"
            | "ARRAY_LENGTH"
            | "CARDINALITY"
            | "ARRAY_POSITION"
            | "ARRAY_UPPER"
            | "ARRAY_LOWER"
            | "ARRAY_NDIMS"
            | "PG_BACKEND_PID"
            | "UUID_EXTRACT_VERSION"
            | "JSON_ARRAY_LENGTH"
            | "JSONB_ARRAY_LENGTH"
            | "NUM_NULLS"
            | "NUM_NONNULLS"
            | "INET_SERVER_PORT"
            | "INET_CLIENT_PORT" => "INTEGER",
            "TXID_CURRENT" | "PG_CURRENT_XACT_ID" | "NEXTVAL" | "CURRVAL" | "LASTVAL"
            | "SETVAL" => "BIGINT",
            "PG_GET_SERIAL_SEQUENCE" => "TEXT",
            "RANDOM" | "PI" | "DATE_PART" => "DOUBLE PRECISION",
            "NOW"
            | "CURRENT_TIMESTAMP"
            | "TRANSACTION_TIMESTAMP"
            | "STATEMENT_TIMESTAMP"
            | "CLOCK_TIMESTAMP"
            | "TO_TIMESTAMP" => "TIMESTAMPTZ",
            "LOCALTIMESTAMP" | "MAKE_TIMESTAMP" => "TIMESTAMP",
            "CURRENT_DATE" | "MAKE_DATE" => "DATE",
            "LOCALTIME" => "TIME",
            "CURRENT_TIME" => "TIMETZ",
            "AGE" => "INTERVAL",
            "DATE_TRUNC" => {
                return Some(
                    arg_types
                        .get(1)
                        .cloned()
                        .flatten()
                        .unwrap_or_else(|| "TIMESTAMP".into()),
                );
            }
            "GEN_RANDOM_UUID" | "UUIDV4" | "UUIDV7" => "UUID",
            "TO_JSONB" | "JSONB_BUILD_OBJECT" | "JSONB_BUILD_ARRAY" | "JSONB_EXTRACT_PATH"
            | "JSONB_SET" | "JSONB_STRIP_NULLS" => "JSONB",
            "TO_JSON" | "JSON_BUILD_OBJECT" | "JSON_BUILD_ARRAY" | "JSON_EXTRACT_PATH"
            | "JSON_STRIP_NULLS" => "JSON",
            "PG_IS_IN_RECOVERY" | "STARTS_WITH" => "BOOLEAN",
            name if crate::value::is_visibility_fn(name) => "BOOLEAN",
            "UPPER"
            | "LOWER"
            | "CASEFOLD"
            | "INITCAP"
            | "SUBSTR"
            | "SUBSTRING"
            | "LEFT"
            | "RIGHT"
            | "LPAD"
            | "RPAD"
            | "BTRIM"
            | "LTRIM"
            | "RTRIM"
            | "REPLACE"
            | "TRANSLATE"
            | "REPEAT"
            | "REVERSE"
            | "SPLIT_PART"
            | "MD5"
            | "CONCAT"
            | "CONCAT_WS"
            | "FORMAT"
            | "QUOTE_IDENT"
            | "QUOTE_LITERAL"
            | "QUOTE_NULLABLE"
            | "CHR"
            | "TO_HEX"
            | "REGEXP_REPLACE"
            | "OVERLAY"
            | "VERSION"
            | "CURRENT_DATABASE"
            | "CURRENT_SCHEMA"
            | "CURRENT_SETTING"
            | "PG_TYPEOF"
            | "PG_SIZE_PRETTY"
            | "PG_ENCODING_TO_CHAR"
            | "PG_CLIENT_ENCODING"
            | "ARRAY_TO_STRING"
            | "JSON_TYPEOF"
            | "JSONB_TYPEOF"
            | "JSON_EXTRACT_PATH_TEXT"
            | "JSONB_EXTRACT_PATH_TEXT"
            | "JSONB_PRETTY"
            | "FORMAT_TYPE"
            | "PG_GET_EXPR"
            | "PG_GET_USERBYID"
            | "OBJ_DESCRIPTION"
            | "COL_DESCRIPTION"
            | "SHOBJ_DESCRIPTION" => "TEXT",
            "CURRENT_USER" | "SESSION_USER" | "CURRENT_ROLE" | "USER" | "CURRENT_CATALOG" => "NAME",
            "COALESCE" | "NULLIF" | "GREATEST" | "LEAST" => return first_known(),
            "ARRAY" => return first_known().map(|element| format!("{element}[]")),
            _ => return None,
        }
        .to_string(),
    )
}

/// Calls a built-in function. Unknown names and wrong argument counts fail the
/// statement.
pub(crate) fn call(name: &str, args: &[Value]) -> Value {
    if !NON_STRICT.contains(&name) && args.iter().any(|a| matches!(a, Value::Null)) {
        return Value::Null;
    }
    match dispatch(name, args) {
        Some(value) => value,
        None => raise(format!(
            "function {}({}) does not exist",
            name.to_ascii_lowercase(),
            args.iter()
                .map(crate::value::value_type_name)
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

/// `(expr)` without the parentheses when they enclose all of it.
fn unwrap_parens(expr: &str) -> Option<&str> {
    let inner = expr.strip_prefix('(')?.strip_suffix(')')?;
    let mut depth = 0i32;
    let mut quoted = false;
    for c in inner.chars() {
        match c {
            '\'' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => {
                depth -= 1;
                if depth < 0 {
                    return None;
                }
            }
            _ => {}
        }
    }
    (depth == 0).then_some(inner)
}

/// Text the catalog gives for an object, or NULL when there is none.
fn catalog_text(
    describe: impl FnOnce(&dyn nodus_catalog::CatalogReader) -> Option<String>,
) -> Value {
    session_env::with(|env| env.and_then(|e| e.catalog.clone()))
        .and_then(|catalog| describe(catalog.as_ref()))
        .map_or(Value::Null, Value::Text)
}

fn text(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        other => render(other),
    }
}

fn int(v: &Value) -> Option<i64> {
    match v {
        Value::Int(i) => Some(*i),
        Value::Float(f) => Some(f.round_ties_even() as i64),
        Value::Numeric(d) => {
            use rust_decimal::prelude::ToPrimitive;
            d.round_dp_with_strategy(0, rust_decimal::RoundingStrategy::MidpointAwayFromZero)
                .to_i64()
        }
        Value::Text(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn num(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Numeric(d) => Some(crate::value::decimal_to_f64(d)),
        Value::Text(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn array(v: &Value) -> Option<Vec<Value>> {
    match v {
        Value::Array(items) => Some(items.clone()),
        Value::Text(s) => crate::value::parse_array_literal(s),
        _ => None,
    }
}

/// A float result, or an error for a non-finite result of finite input.
fn float(x: f64) -> Value {
    if x.is_nan() {
        raise("input is out of range")
    } else {
        Value::Float(x)
    }
}

/// Keeps integer results integral when every numeric input was an integer.
/// `round`/`trunc` of a numeric to `digits` places (negative: to tens,
/// hundreds, ...); rounding takes halves away from zero, and a non-negative
/// `digits` is the result's scale.
fn round_decimal(d: rust_decimal::Decimal, digits: i64, round: bool) -> Value {
    use rust_decimal::RoundingStrategy::{MidpointAwayFromZero, ToZero};
    let strategy = if round { MidpointAwayFromZero } else { ToZero };
    if digits >= 0 {
        let scale = digits.min(28) as u32;
        let mut r = d.round_dp_with_strategy(scale, strategy);
        r.rescale(scale);
        return Value::Numeric(r);
    }
    let factor = match 10i64.checked_pow((-digits).min(18) as u32) {
        Some(f) => rust_decimal::Decimal::from(f),
        None => return Value::Numeric(rust_decimal::Decimal::ZERO),
    };
    match (d / factor)
        .round_dp_with_strategy(0, strategy)
        .checked_mul(factor)
    {
        Some(r) => Value::Numeric(r),
        None => raise("value overflows numeric format"),
    }
}

fn numeric_like(args: &[Value], x: f64) -> Value {
    if args.iter().all(|a| matches!(a, Value::Int(_))) && x.fract() == 0.0 && x.abs() < 9.2e18 {
        Value::Int(x as i64)
    } else {
        Value::Float(x)
    }
}

fn dispatch(name: &str, args: &[Value]) -> Option<Value> {
    let arity = |n: usize| args.len() == n;
    let arg = |i: usize| args.get(i).unwrap_or(&Value::Null);
    Some(match name {
        // ---- Strings --------------------------------------------------------
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" if arity(1) => {
            Value::Int(text(arg(0)).chars().count() as i64)
        }
        "OCTET_LENGTH" if arity(1) => Value::Int(text(arg(0)).len() as i64),
        "BIT_LENGTH" if arity(1) => Value::Int(text(arg(0)).len() as i64 * 8),
        "UPPER" if arity(1) => Value::Text(text(arg(0)).to_uppercase()),
        "LOWER" | "CASEFOLD" if arity(1) => Value::Text(text(arg(0)).to_lowercase()),
        "INITCAP" if arity(1) => {
            let mut out = String::new();
            let mut word_start = true;
            for c in text(arg(0)).chars() {
                if c.is_alphanumeric() {
                    out.extend(if word_start {
                        c.to_uppercase().collect::<Vec<_>>()
                    } else {
                        c.to_lowercase().collect::<Vec<_>>()
                    });
                    word_start = false;
                } else {
                    out.push(c);
                    word_start = true;
                }
            }
            Value::Text(out)
        }
        "SUBSTR" | "SUBSTRING" if arity(2) || arity(3) => {
            let chars: Vec<char> = text(arg(0)).chars().collect();
            let start = int(arg(1))?;
            // Positions before 1 still consume the requested length.
            let end = match args.get(2) {
                Some(len) => {
                    let len = int(len)?;
                    if len < 0 {
                        return Some(raise("negative substring length not allowed"));
                    }
                    start.saturating_add(len)
                }
                None => i64::MAX,
            };
            let from = (start.max(1) - 1) as usize;
            let to = (end.max(1) - 1).min(chars.len() as i64) as usize;
            Value::Text(
                chars
                    .get(from..to.max(from))
                    .unwrap_or(&[])
                    .iter()
                    .collect(),
            )
        }
        "STRPOS" if arity(2) => {
            let (s, sub) = (text(arg(0)), text(arg(1)));
            Value::Int(
                s.find(&sub)
                    .map_or(0, |byte| s[..byte].chars().count() as i64 + 1),
            )
        }
        "OVERLAY" if arity(3) || arity(4) => {
            let s: Vec<char> = text(arg(0)).chars().collect();
            let replacement = text(arg(1));
            let from = int(arg(2))?.max(1) as usize - 1;
            let len = match args.get(3) {
                Some(l) => int(l)?.max(0) as usize,
                None => replacement.chars().count(),
            };
            let head: String = s.iter().take(from).collect();
            let tail: String = s.iter().skip(from + len).collect();
            Value::Text(format!("{head}{replacement}{tail}"))
        }
        "TRIM" | "BTRIM" | "LTRIM" | "RTRIM" if arity(1) || arity(2) => {
            let s = text(arg(0));
            let chars: Vec<char> = match args.get(1) {
                Some(set) => text(set).chars().collect(),
                None => vec![' '],
            };
            let trimmed = match name {
                "LTRIM" => s.trim_start_matches(&chars[..]),
                "RTRIM" => s.trim_end_matches(&chars[..]),
                _ => s.trim_matches(&chars[..]),
            };
            Value::Text(trimmed.to_string())
        }
        "LPAD" | "RPAD" if arity(2) || arity(3) => {
            let s: Vec<char> = text(arg(0)).chars().collect();
            let len = int(arg(1))?.max(0) as usize;
            let fill: Vec<char> = match args.get(2) {
                Some(f) => text(f).chars().collect(),
                None => vec![' '],
            };
            if s.len() >= len || fill.is_empty() {
                return Some(Value::Text(s.iter().take(len).collect()));
            }
            let pad: String = fill.iter().cycle().take(len - s.len()).collect();
            let s: String = s.into_iter().collect();
            Value::Text(if name == "LPAD" {
                format!("{pad}{s}")
            } else {
                format!("{s}{pad}")
            })
        }
        "REPLACE" if arity(3) => {
            let (s, from, to) = (text(arg(0)), text(arg(1)), text(arg(2)));
            Value::Text(if from.is_empty() {
                s
            } else {
                s.replace(&from, &to)
            })
        }
        "TRANSLATE" if arity(3) => {
            let from: Vec<char> = text(arg(1)).chars().collect();
            let to: Vec<char> = text(arg(2)).chars().collect();
            Value::Text(
                text(arg(0))
                    .chars()
                    .filter_map(|c| match from.iter().position(|&f| f == c) {
                        Some(i) => to.get(i).copied(),
                        None => Some(c),
                    })
                    .collect(),
            )
        }
        "REPEAT" if arity(2) => Value::Text(text(arg(0)).repeat(int(arg(1))?.max(0) as usize)),
        "REVERSE" if arity(1) => Value::Text(text(arg(0)).chars().rev().collect()),
        "MD5" if arity(1) => {
            use md5::Digest;
            let digest = md5::Md5::digest(text(arg(0)).as_bytes());
            Value::Text(digest.iter().map(|b| format!("{b:02x}")).collect())
        }
        "SPLIT_PART" if arity(3) => {
            let (s, delim) = (text(arg(0)), text(arg(1)));
            let n = int(arg(2))?;
            if n == 0 {
                return Some(raise("field position must not be zero"));
            }
            let parts: Vec<&str> = if delim.is_empty() {
                vec![s.as_str()]
            } else {
                s.split(delim.as_str()).collect()
            };
            let idx = if n > 0 { n - 1 } else { parts.len() as i64 + n };
            Value::Text(
                usize::try_from(idx)
                    .ok()
                    .and_then(|i| parts.get(i))
                    .unwrap_or(&"")
                    .to_string(),
            )
        }
        "LEFT" | "RIGHT" if arity(2) => {
            let chars: Vec<char> = text(arg(0)).chars().collect();
            let n = int(arg(1))?;
            let len = chars.len() as i64;
            let keep = if n >= 0 { n.min(len) } else { (len + n).max(0) } as usize;
            Value::Text(if name == "LEFT" {
                chars[..keep].iter().collect()
            } else {
                chars[chars.len() - keep..].iter().collect()
            })
        }
        "CONCAT" => Value::Text(
            args.iter()
                .filter(|v| !matches!(v, Value::Null))
                .map(text)
                .collect(),
        ),
        "CONCAT_WS" if !args.is_empty() => match arg(0) {
            Value::Null => Value::Null,
            sep => Value::Text(
                args[1..]
                    .iter()
                    .filter(|v| !matches!(v, Value::Null))
                    .map(text)
                    .collect::<Vec<_>>()
                    .join(&text(sep)),
            ),
        },
        "FORMAT" if !args.is_empty() => match arg(0) {
            Value::Null => Value::Null,
            fmt => format_text(&text(fmt), &args[1..]),
        },
        "QUOTE_IDENT" if arity(1) => Value::Text(quote_ident(&text(arg(0)))),
        "QUOTE_LITERAL" if arity(1) => Value::Text(quote_literal(&text(arg(0)))),
        "QUOTE_NULLABLE" if arity(1) => Value::Text(match arg(0) {
            Value::Null => "NULL".to_string(),
            v => quote_literal(&text(v)),
        }),
        "ASCII" if arity(1) => Value::Int(text(arg(0)).chars().next().map_or(0, |c| c as i64)),
        "CHR" if arity(1) => match u32::try_from(int(arg(0))?).ok().and_then(char::from_u32) {
            Some(c) if c != '\0' => Value::Text(c.to_string()),
            _ => raise("requested character is not valid"),
        },
        "TO_HEX" if arity(1) => Value::Text(format!("{:x}", int(arg(0))?)),
        "TO_BIN" if arity(1) => Value::Text(format!("{:b}", int(arg(0))?)),
        "TO_OCT" if arity(1) => Value::Text(format!("{:o}", int(arg(0))?)),
        "STARTS_WITH" if arity(2) => Value::Bool(text(arg(0)).starts_with(&text(arg(1)))),
        "REGEXP_REPLACE" if (3..=4).contains(&args.len()) => {
            let flags = args.get(3).map(text).unwrap_or_default();
            let Some(re) = regex_with_flags(&text(arg(1)), &flags) else {
                return Some(raise(format!(
                    "invalid regular expression: {}",
                    text(arg(1))
                )));
            };
            let replacement = pg_replacement(&text(arg(2)));
            let s = text(arg(0));
            Value::Text(if flags.contains('g') {
                re.replace_all(&s, replacement.as_str()).into_owned()
            } else {
                re.replace(&s, replacement.as_str()).into_owned()
            })
        }
        "REGEXP_MATCH" if arity(2) || arity(3) => {
            let flags = args.get(2).map(text).unwrap_or_default();
            let Some(re) = regex_with_flags(&text(arg(1)), &flags) else {
                return Some(raise(format!(
                    "invalid regular expression: {}",
                    text(arg(1))
                )));
            };
            let s = text(arg(0));
            match re.captures(&s) {
                None => Value::Null,
                Some(caps) if caps.len() == 1 => {
                    Value::Array(vec![Value::Text(caps[0].to_string())])
                }
                Some(caps) => Value::Array(
                    caps.iter()
                        .skip(1)
                        .map(|m| m.map_or(Value::Null, |m| Value::Text(m.as_str().to_string())))
                        .collect(),
                ),
            }
        }
        "REGEXP_LIKE" if arity(2) || arity(3) => {
            let flags = args.get(2).map(text).unwrap_or_default();
            match regex_with_flags(&text(arg(1)), &flags) {
                Some(re) => Value::Bool(re.is_match(&text(arg(0)))),
                None => raise(format!("invalid regular expression: {}", text(arg(1)))),
            }
        }
        "REGEXP_COUNT" if arity(2) => match regex_with_flags(&text(arg(1)), "") {
            Some(re) => Value::Int(re.find_iter(&text(arg(0))).count() as i64),
            None => raise(format!("invalid regular expression: {}", text(arg(1)))),
        },
        "REGEXP_SUBSTR" if arity(2) => match regex_with_flags(&text(arg(1)), "") {
            Some(re) => re
                .find(&text(arg(0)))
                .map_or(Value::Null, |m| Value::Text(m.as_str().to_string())),
            None => raise(format!("invalid regular expression: {}", text(arg(1)))),
        },
        "REGEXP_SPLIT_TO_ARRAY" if arity(2) || arity(3) => {
            let flags = args.get(2).map(text).unwrap_or_default();
            match regex_with_flags(&text(arg(1)), &flags) {
                Some(re) => Value::Array(
                    re.split(&text(arg(0)))
                        .map(|p| Value::Text(p.to_string()))
                        .collect(),
                ),
                None => raise(format!("invalid regular expression: {}", text(arg(1)))),
            }
        }
        "STRING_TO_ARRAY" if arity(2) || arity(3) => {
            if matches!(arg(0), Value::Null) {
                return Some(Value::Null);
            }
            let s = text(arg(0));
            let null_str = args.get(2).filter(|v| !matches!(v, Value::Null)).map(text);
            let as_value = |p: String| match &null_str {
                Some(n) if &p == n => Value::Null,
                _ => Value::Text(p),
            };
            if s.is_empty() {
                return Some(Value::Array(Vec::new()));
            }
            Value::Array(match arg(1) {
                Value::Null => s.chars().map(|c| as_value(c.to_string())).collect(),
                d if text(d).is_empty() => vec![as_value(s)],
                d => s
                    .split(text(d).as_str())
                    .map(|p| as_value(p.to_string()))
                    .collect(),
            })
        }
        "ARRAY_TO_STRING" if arity(2) || arity(3) => {
            let (Some(items), false) = (array(arg(0)), matches!(arg(1), Value::Null)) else {
                return Some(Value::Null);
            };
            let null_str = args.get(2).filter(|v| !matches!(v, Value::Null)).map(text);
            let mut flat = Vec::new();
            flatten(items, &mut flat);
            Value::Text(
                flat.iter()
                    .filter_map(|v| match v {
                        Value::Null => null_str.clone(),
                        v => Some(text(v)),
                    })
                    .collect::<Vec<_>>()
                    .join(&text(arg(1))),
            )
        }

        // ---- Conditionals ---------------------------------------------------
        // `ARRAY[a, b, ...]` over per-row values.
        "ARRAY" => Value::Array(args.to_vec()),
        "COALESCE" => args
            .iter()
            .find(|v| !matches!(v, Value::Null))
            .cloned()
            .unwrap_or(Value::Null),
        "NULLIF" if arity(2) => {
            if !matches!(arg(0), Value::Null)
                && !matches!(arg(1), Value::Null)
                && crate::planner::apply_binary_op(
                    crate::ScalarBinaryOp::Eq,
                    arg(0).clone(),
                    arg(1).clone(),
                ) == Value::Bool(true)
            {
                Value::Null
            } else {
                arg(0).clone()
            }
        }
        "GREATEST" | "LEAST" if !args.is_empty() => {
            let mut best: Option<&Value> = None;
            for v in args.iter().filter(|v| !matches!(v, Value::Null)) {
                let better = match best {
                    None => true,
                    Some(b) => {
                        let op = if name == "GREATEST" {
                            crate::ScalarBinaryOp::Gt
                        } else {
                            crate::ScalarBinaryOp::Lt
                        };
                        crate::planner::apply_binary_op(op, v.clone(), b.clone())
                            == Value::Bool(true)
                    }
                };
                if better {
                    best = Some(v);
                }
            }
            best.cloned().unwrap_or(Value::Null)
        }
        "NUM_NULLS" => Value::Int(args.iter().filter(|v| matches!(v, Value::Null)).count() as i64),
        "NUM_NONNULLS" => {
            Value::Int(args.iter().filter(|v| !matches!(v, Value::Null)).count() as i64)
        }

        // ---- Math -------------------------------------------------------------
        "ABS" if arity(1) => match arg(0) {
            Value::Int(i) => i
                .checked_abs()
                .map_or_else(|| raise("bigint out of range"), Value::Int),
            Value::Numeric(d) => Value::Numeric(d.abs()),
            v => Value::Float(num(v)?.abs()),
        },
        // On a numeric these keep exact decimals, as PostgreSQL's numeric
        // variants do.
        "SIGN" if arity(1) && matches!(arg(0), Value::Numeric(_)) => {
            let Value::Numeric(d) = arg(0) else {
                return None;
            };
            Value::Numeric(if d.is_zero() {
                rust_decimal::Decimal::ZERO
            } else if d.is_sign_negative() {
                rust_decimal::Decimal::NEGATIVE_ONE
            } else {
                rust_decimal::Decimal::ONE
            })
        }
        "CEIL" | "CEILING" | "FLOOR" if arity(1) && matches!(arg(0), Value::Numeric(_)) => {
            let Value::Numeric(d) = arg(0) else {
                return None;
            };
            Value::Numeric(if name == "FLOOR" { d.floor() } else { d.ceil() })
        }
        "ROUND" | "TRUNC"
            if (arity(1) || arity(2))
                && (matches!(arg(0), Value::Numeric(_))
                    || (arity(2) && matches!(arg(0), Value::Int(_)))) =>
        {
            let d = match arg(0) {
                Value::Numeric(d) => *d,
                other => rust_decimal::Decimal::from(int(other)?),
            };
            let digits = match args.get(1) {
                Some(n) => int(n)?,
                None => 0,
            };
            round_decimal(d, digits, name == "ROUND")
        }
        "SIGN" if arity(1) => {
            numeric_like(args, num(arg(0))?.signum() * f64::from(num(arg(0))? != 0.0))
        }
        "CEIL" | "CEILING" if arity(1) => numeric_like(args, num(arg(0))?.ceil()),
        "FLOOR" if arity(1) => numeric_like(args, num(arg(0))?.floor()),
        "ROUND" | "TRUNC" if arity(1) || arity(2) => {
            let x = num(arg(0))?;
            let digits = match args.get(1) {
                Some(d) => int(d)?,
                None => 0,
            };
            let factor = 10f64.powi(digits.clamp(-300, 300) as i32);
            let scaled = x * factor;
            // A float rounds half to even, as PostgreSQL's round(float8).
            let r = if name == "ROUND" {
                scaled.round_ties_even()
            } else {
                scaled.trunc()
            } / factor;
            if matches!(arg(0), Value::Int(_)) && digits >= 0 {
                arg(0).clone()
            } else {
                Value::Float(r)
            }
        }
        "MOD" if arity(2) => crate::planner::apply_binary_op(
            crate::ScalarBinaryOp::Mod,
            arg(0).clone(),
            arg(1).clone(),
        ),
        "DIV" if arity(2) => {
            let (a, b) = (num(arg(0))?, num(arg(1))?);
            if b == 0.0 {
                raise("division by zero")
            } else {
                numeric_like(&[Value::Int(0)], (a / b).trunc())
            }
        }
        "POWER" | "POW" if arity(2) => {
            let (a, b) = (num(arg(0))?, num(arg(1))?);
            if a == 0.0 && b < 0.0 {
                return Some(raise("zero raised to a negative power is undefined"));
            }
            if a < 0.0 && b.fract() != 0.0 {
                return Some(raise(
                    "a negative number raised to a non-integer power yields a complex result",
                ));
            }
            Value::Float(a.powf(b))
        }
        "SQRT" if arity(1) => {
            let x = num(arg(0))?;
            if x < 0.0 {
                raise("cannot take square root of a negative number")
            } else {
                Value::Float(x.sqrt())
            }
        }
        "CBRT" if arity(1) => Value::Float(num(arg(0))?.cbrt()),
        "EXP" if arity(1) => float(num(arg(0))?.exp()),
        "LN" | "LOG" | "LOG10" if arity(1) => {
            let x = num(arg(0))?;
            if x == 0.0 {
                raise("cannot take logarithm of zero")
            } else if x < 0.0 {
                raise("cannot take logarithm of a negative number")
            } else if name == "LN" {
                Value::Float(x.ln())
            } else {
                Value::Float(x.log10())
            }
        }
        "LOG" if arity(2) => {
            let (base, x) = (num(arg(0))?, num(arg(1))?);
            if base <= 0.0 || x <= 0.0 {
                raise("cannot take logarithm of zero or a negative number")
            } else if base == 1.0 {
                raise("division by zero")
            } else {
                Value::Float(x.ln() / base.ln())
            }
        }
        "PI" if arity(0) => Value::Float(std::f64::consts::PI),
        "DEGREES" if arity(1) => Value::Float(num(arg(0))?.to_degrees()),
        "RADIANS" if arity(1) => Value::Float(num(arg(0))?.to_radians()),
        "SIN" | "COS" | "TAN" | "COT" | "ASIN" | "ACOS" | "ATAN" | "SINH" | "COSH" | "TANH"
            if arity(1) =>
        {
            let x = num(arg(0))?;
            if matches!(name, "ASIN" | "ACOS") && !(-1.0..=1.0).contains(&x) {
                return Some(raise("input is out of range"));
            }
            Value::Float(match name {
                "SIN" => x.sin(),
                "COS" => x.cos(),
                "TAN" => x.tan(),
                "COT" => 1.0 / x.tan(),
                "ASIN" => x.asin(),
                "ACOS" => x.acos(),
                "ATAN" => x.atan(),
                "SINH" => x.sinh(),
                "COSH" => x.cosh(),
                _ => x.tanh(),
            })
        }
        "ATAN2" if arity(2) => Value::Float(num(arg(0))?.atan2(num(arg(1))?)),
        "GCD" | "LCM" if arity(2) => {
            let (a, b) = (int(arg(0))?.unsigned_abs(), int(arg(1))?.unsigned_abs());
            let gcd = {
                let (mut x, mut y) = (a, b);
                while y != 0 {
                    (x, y) = (y, x % y);
                }
                x
            };
            let out = if name == "GCD" {
                gcd
            } else if a == 0 || b == 0 {
                0
            } else {
                a / gcd * b
            };
            i64::try_from(out).map_or_else(|_| raise("bigint out of range"), Value::Int)
        }
        "FACTORIAL" if arity(1) => {
            let n = int(arg(0))?;
            if n < 0 {
                return Some(raise("factorial of a negative number is undefined"));
            }
            (1..=n)
                .try_fold(1_i64, |acc, k| acc.checked_mul(k))
                .map_or_else(|| raise("bigint out of range"), Value::Int)
        }
        "RANDOM" if arity(0) => Value::Float(random_unit()),
        "RANDOM" if arity(2) => {
            let (lo, hi) = (int(arg(0))?, int(arg(1))?);
            if lo > hi {
                return Some(raise(
                    "lower bound must be less than or equal to upper bound",
                ));
            }
            let span = (hi - lo) as f64 + 1.0;
            Value::Int(lo + (random_unit() * span).floor() as i64)
        }
        "WIDTH_BUCKET" if arity(4) => {
            let (x, lo, hi, n) = (num(arg(0))?, num(arg(1))?, num(arg(2))?, int(arg(3))?);
            if n <= 0 {
                return Some(raise("count must be greater than zero"));
            }
            if lo == hi {
                return Some(raise("lower bound cannot equal upper bound"));
            }
            let bucket = if lo < hi {
                if x < lo {
                    0
                } else if x >= hi {
                    n + 1
                } else {
                    ((x - lo) / (hi - lo) * n as f64).floor() as i64 + 1
                }
            } else if x > lo {
                0
            } else if x <= hi {
                n + 1
            } else {
                ((lo - x) / (lo - hi) * n as f64).floor() as i64 + 1
            };
            Value::Int(bucket)
        }

        // ---- Dates and times ----------------------------------------------------
        "NOW" | "CURRENT_TIMESTAMP" | "TRANSACTION_TIMESTAMP" if arity(0) => {
            timestamp(session_time(|e| e.transaction_micros)?, true)
        }
        "STATEMENT_TIMESTAMP" if arity(0) => timestamp(session_time(|e| e.statement_micros)?, true),
        "CLOCK_TIMESTAMP" if arity(0) => timestamp(session_env::wall_micros(), true),
        "LOCALTIMESTAMP" if arity(0) => timestamp(session_time(|e| e.transaction_micros)?, false),
        "CURRENT_DATE" if arity(0) => {
            let ts = timestamp(session_time(|e| e.transaction_micros)?, false);
            Value::Text(text(&ts)[..10].to_string())
        }
        "CURRENT_TIME" | "LOCALTIME" if arity(0) => {
            let ts = text(&timestamp(session_time(|e| e.transaction_micros)?, false));
            let time = &ts[11..];
            Value::Text(if name == "CURRENT_TIME" {
                format!("{time}+00")
            } else {
                time.to_string()
            })
        }
        "DATE_TRUNC" if arity(2) => crate::value::date_trunc_text(&text(arg(0)), &text(arg(1))),
        "AGE" if arity(2) => crate::value::age_text(&text(arg(0)), &text(arg(1))),
        "AGE" if arity(1) => {
            let today = text(&timestamp(session_time(|e| e.transaction_micros)?, false));
            crate::value::age_text(&format!("{} 00:00:00", &today[..10]), &text(arg(0)))
        }
        "DATE_PART" if arity(2) => {
            crate::planner::extract_datetime_field(arg(1), &text(arg(0)).to_ascii_uppercase())
        }
        "MAKE_DATE" if arity(3) => {
            let (y, m, d) = (int(arg(0))?, int(arg(1))?, int(arg(2))?);
            match chrono::NaiveDate::from_ymd_opt(y as i32, m as u32, d as u32) {
                Some(date) => Value::Text(date.format("%Y-%m-%d").to_string()),
                None => raise(format!("date field value out of range: {y}-{m:02}-{d:02}")),
            }
        }
        "MAKE_TIMESTAMP" if arity(6) => {
            let (y, mo, d, h, mi) = (
                int(arg(0))?,
                int(arg(1))?,
                int(arg(2))?,
                int(arg(3))?,
                int(arg(4))?,
            );
            let secs = num(arg(5))?;
            let micros = (secs.fract() * 1_000_000.0).round() as u32;
            match chrono::NaiveDate::from_ymd_opt(y as i32, mo as u32, d as u32).and_then(|date| {
                date.and_hms_micro_opt(h as u32, mi as u32, secs.trunc() as u32, micros)
            }) {
                Some(ts) => Value::Text(crate::value::format_timestamp(ts, false)),
                None => raise("date/time field value out of range"),
            }
        }
        "TO_TIMESTAMP" if arity(1) => {
            let secs = num(arg(0))?;
            timestamp((secs * 1_000_000.0).round() as i64, true)
        }

        // ---- Session and system ---------------------------------------------------
        "VERSION" if arity(0) => Value::Text(format!(
            "PostgreSQL {} (NodusDB)",
            session_env::setting("server_version").unwrap_or_default()
        )),
        "CURRENT_USER" | "SESSION_USER" | "CURRENT_ROLE" | "USER" if arity(0) => {
            match session_env::with(|e| e.map(|e| e.user.clone())) {
                Some(user) => Value::Text(user),
                None => session_unavailable(name),
            }
        }
        "CURRENT_DATABASE" | "CURRENT_CATALOG" if arity(0) => Value::Text("default".to_string()),
        "CURRENT_SCHEMA" if arity(0) => search_path()
            .into_iter()
            .next()
            .map_or(Value::Null, Value::Text),
        "CURRENT_SCHEMAS" if arity(1) => {
            let mut schemas = search_path();
            if matches!(arg(0), Value::Bool(true)) {
                schemas.insert(0, "pg_catalog".to_string());
            }
            Value::Array(schemas.into_iter().map(Value::Text).collect())
        }
        "CURRENT_SETTING" if arity(1) || arity(2) => {
            if matches!(arg(0), Value::Null) {
                return Some(Value::Null);
            }
            let name = text(arg(0));
            match session_env::setting(&name) {
                Some(value) => Value::Text(value),
                None if matches!(arg(1), Value::Bool(true)) => Value::Null,
                None => raise(format!("unrecognized configuration parameter \"{name}\"")),
            }
        }
        "PG_BACKEND_PID" if arity(0) => match session_env::with(|e| e.map(|e| e.backend_pid)) {
            Some(pid) => Value::Int(pid),
            None => session_unavailable(name),
        },
        "TXID_CURRENT" | "PG_CURRENT_XACT_ID" if arity(0) => {
            Value::Int(session_time(|e| e.transaction_micros)?)
        }
        "PG_TYPEOF" if arity(1) => Value::Text(
            match arg(0) {
                Value::Int(i) if i32::try_from(*i).is_ok() => "integer",
                Value::Int(_) => "bigint",
                Value::Float(_) => "double precision",
                Value::Numeric(_) => "numeric",
                Value::Text(_) => "text",
                Value::Bool(_) => "boolean",
                Value::Jsonb(_) => "jsonb",
                Value::Array(_) => "text[]",
                Value::Null => "unknown",
            }
            .to_string(),
        ),
        "PG_SIZE_PRETTY" if arity(1) => Value::Text(size_pretty(int(arg(0))?)),
        "PG_ENCODING_TO_CHAR" if arity(1) => {
            Value::Text(if int(arg(0))? == 6 { "UTF8" } else { "" }.to_string())
        }
        "PG_CLIENT_ENCODING" if arity(0) => Value::Text("UTF8".to_string()),
        "PG_IS_IN_RECOVERY" if arity(0) => Value::Bool(false),
        // ---- Sequences ---------------------------------------------------------
        // `__IDENTITY__(sequence, always)` is an identity column's default.
        "NEXTVAL" if arity(1) => {
            sequence_op(|store, session| store.nextval(session, &text(arg(0))))
        }
        "__IDENTITY__" if arity(2) => {
            sequence_op(|store, session| store.nextval(session, &text(arg(0))))
        }
        "CURRVAL" if arity(1) => {
            sequence_op(|store, session| store.currval(session, &text(arg(0))))
        }
        "LASTVAL" if arity(0) => sequence_op(|store, session| store.lastval(session)),
        "SETVAL" if arity(2) || arity(3) => {
            let value = int(arg(1))?;
            let is_called = !matches!(arg(2), Value::Bool(false));
            sequence_op(|store, session| store.setval(session, &text(arg(0)), value, is_called))
        }
        "PG_GET_SERIAL_SEQUENCE" if arity(2) => {
            let table = text(arg(0));
            let column = text(arg(1)).to_ascii_lowercase();
            match session_env::with(|e| e.and_then(|e| e.sequences.clone())) {
                Some(store) => match store.owned_sequence(&table, &column) {
                    Ok(Some(name)) => Value::Text(name),
                    Ok(None) => Value::Null,
                    Err(e) => raise(e.to_string()),
                },
                None => session_unavailable(name),
            }
        }
        "PG_SLEEP" if arity(1) => {
            let seconds = num(arg(0))?;
            if seconds > 0.0 {
                std::thread::sleep(std::time::Duration::from_secs_f64(seconds.min(1e9)));
            }
            Value::Null
        }
        // The catalog exposes one role, the bootstrap superuser (OID 10).
        "PG_GET_USERBYID" if arity(1) => Value::Text(match int(arg(0))? {
            10 => "nodus".to_string(),
            oid => format!("unknown (OID={oid})"),
        }),
        // Connections are not described to the executor; PostgreSQL reports
        // NULL the same way for a Unix-socket connection.
        "INET_SERVER_ADDR" | "INET_SERVER_PORT" | "INET_CLIENT_ADDR" | "INET_CLIENT_PORT"
            if arity(0) =>
        {
            Value::Null
        }
        // NodusDB has no COMMENT ON, so no object has a description.
        "OBJ_DESCRIPTION" if arity(1) || arity(2) => Value::Null,
        "COL_DESCRIPTION" | "SHOBJ_DESCRIPTION" if arity(2) => Value::Null,
        "FORMAT_TYPE" if arity(2) => match arg(0) {
            Value::Null => Value::Null,
            oid => Value::Text(format_type(int(oid)?, int(arg(1)).filter(|m| *m >= 0))),
        },
        // Expressions are stored as their SQL text; pretty-printed, without
        // the parentheses around the whole of it.
        "PG_GET_EXPR" if arity(2) || arity(3) => {
            let expr = text(arg(0));
            match (
                arity(3) && matches!(arg(2), Value::Bool(true)),
                unwrap_parens(&expr),
            ) {
                (true, Some(inner)) => Value::Text(inner.to_string()),
                _ => Value::Text(expr),
            }
        }
        "PG_GET_INDEXDEF" if arity(1) || arity(3) => {
            let column = if arity(3) { int(arg(1))? } else { 0 };
            let pretty = arity(3) && matches!(arg(2), Value::Bool(true));
            catalog_text(|catalog| {
                crate::MemExecutor::index_definition(catalog, int(arg(0))?, column, pretty)
            })
        }
        // By OID or by (possibly qualified) name.
        "PG_GET_VIEWDEF" if arity(1) || arity(2) => catalog_text(|catalog| {
            let oid = match arg(0) {
                Value::Text(name) if name.trim().parse::<i64>().is_err() => {
                    crate::MemExecutor::relation_oid(catalog, name)?
                }
                other => int(other)?,
            };
            crate::MemExecutor::view_definition(catalog, oid)
        }),
        "__OBJECT_NAME__" if arity(2) => {
            let oid = int(arg(0))?;
            let kind = text(arg(1));
            match catalog_text(|catalog| crate::MemExecutor::object_name(catalog, &kind, oid)) {
                Value::Null => Value::Text(oid.to_string()),
                name => name,
            }
        }
        "PG_GET_CONSTRAINTDEF" if arity(1) || arity(2) => {
            let pretty = arity(2) && matches!(arg(1), Value::Bool(true));
            catalog_text(|catalog| {
                crate::MemExecutor::constraint_definition(catalog, int(arg(0))?, pretty)
            })
        }
        // Any table could be published (there are no publications).
        "PG_RELATION_IS_PUBLISHABLE" if arity(1) => Value::Bool(true),
        // There are no extended statistics objects.
        "PG_GET_STATISTICSOBJDEF_COLUMNS" if arity(1) => Value::Null,
        name if crate::value::is_visibility_fn(name) && arity(1) => Value::Bool(true),

        // ---- UUIDs ------------------------------------------------------------------
        "GEN_RANDOM_UUID" | "UUIDV4" if arity(0) => Value::Text(uuid::Uuid::new_v4().to_string()),
        "UUIDV7" if arity(0) => Value::Text(uuid_v7(session_env::wall_micros() / 1000).to_string()),
        "UUID_EXTRACT_VERSION" if arity(1) => match uuid::Uuid::parse_str(text(arg(0)).trim()) {
            Ok(u) if u.get_variant() == uuid::Variant::RFC4122 => {
                Value::Int(u.get_version_num() as i64)
            }
            Ok(_) => Value::Null,
            Err(_) => raise(format!(
                "invalid input syntax for type uuid: \"{}\"",
                text(arg(0))
            )),
        },

        // ---- JSON ---------------------------------------------------------------------
        "TO_JSON" | "TO_JSONB" if arity(1) => match arg(0) {
            Value::Null => Value::Null,
            v => Value::Jsonb(to_json(v)),
        },
        "JSON_BUILD_OBJECT" | "JSONB_BUILD_OBJECT" => {
            if args.len() % 2 != 0 {
                return Some(raise("argument list must have even number of elements"));
            }
            let mut map = serde_json::Map::new();
            for pair in args.chunks(2) {
                if matches!(pair[0], Value::Null) {
                    return Some(raise("null value not allowed for object key"));
                }
                map.insert(text(&pair[0]), to_json(&pair[1]));
            }
            Value::Jsonb(serde_json::Value::Object(map))
        }
        "JSON_BUILD_ARRAY" | "JSONB_BUILD_ARRAY" => {
            Value::Jsonb(serde_json::Value::Array(args.iter().map(to_json).collect()))
        }
        "JSON_TYPEOF" | "JSONB_TYPEOF" if arity(1) => {
            let json = json_arg(arg(0))?;
            Value::Text(
                match json {
                    serde_json::Value::Object(_) => "object",
                    serde_json::Value::Array(_) => "array",
                    serde_json::Value::String(_) => "string",
                    serde_json::Value::Number(_) => "number",
                    serde_json::Value::Bool(_) => "boolean",
                    serde_json::Value::Null => "null",
                }
                .to_string(),
            )
        }
        "JSON_ARRAY_LENGTH" | "JSONB_ARRAY_LENGTH" if arity(1) => match json_arg(arg(0))? {
            serde_json::Value::Array(items) => Value::Int(items.len() as i64),
            _ => raise("cannot get array length of a non-array"),
        },
        "JSON_EXTRACT_PATH"
        | "JSONB_EXTRACT_PATH"
        | "JSON_EXTRACT_PATH_TEXT"
        | "JSONB_EXTRACT_PATH_TEXT"
            if !args.is_empty() =>
        {
            let path = Value::Array(args[1..].to_vec());
            let op = if name.ends_with("_TEXT") {
                crate::ScalarBinaryOp::JsonPathText
            } else {
                crate::ScalarBinaryOp::JsonPath
            };
            crate::planner::apply_binary_op(op, arg(0).clone(), path)
        }
        "JSONB_SET" if arity(3) || arity(4) => {
            let mut json = json_arg(arg(0))?;
            let path: Vec<String> = array(arg(1))?.iter().map(text).collect();
            let create = !matches!(args.get(3), Some(Value::Bool(false)));
            let new_value = to_json(&parse_json_arg(arg(2)));
            json_set(&mut json, &path, new_value, create);
            Value::Jsonb(json)
        }
        "JSONB_STRIP_NULLS" | "JSON_STRIP_NULLS" if arity(1) || arity(2) => {
            let mut json = json_arg(arg(0))?;
            strip_nulls(&mut json, matches!(args.get(1), Some(Value::Bool(true))));
            Value::Jsonb(json)
        }
        "JSONB_PRETTY" if arity(1) => {
            Value::Text(crate::json_text::jsonb_pretty(&json_arg(arg(0))?))
        }

        // An integer result checked against its type's range.
        crate::result_types::INTEGER_RANGE if arity(2) => match arg(0) {
            Value::Int(v) => {
                let ty = text(arg(1));
                let (min, max) = crate::value::integer_range(&ty);
                if (min..=max).contains(v) {
                    Value::Int(*v)
                } else {
                    raise(format!("{} out of range", crate::value::sql_type_name(&ty)))
                }
            }
            other => other.clone(),
        },

        // ---- Subscripts ---------------------------------------------------------------
        "__SUBSCRIPT__" if arity(2) => match arg(0) {
            // `jsonb` subscripting: an object field or an array element.
            Value::Jsonb(json) => {
                let step = match (json, arg(1)) {
                    (serde_json::Value::Object(map), key) => map.get(&text(key)).cloned(),
                    (serde_json::Value::Array(items), index) => int(index).and_then(|i| {
                        let i = if i < 0 { items.len() as i64 + i } else { i };
                        usize::try_from(i).ok().and_then(|i| items.get(i).cloned())
                    }),
                    _ => None,
                };
                step.map_or(Value::Null, Value::Jsonb)
            }
            base => {
                let items = array(base)?;
                let index = int(arg(1))?;
                usize::try_from(index)
                    .ok()
                    .filter(|&i| i >= 1)
                    .and_then(|i| items.get(i - 1).cloned())
                    .unwrap_or(Value::Null)
            }
        },
        "__SLICE__" if arity(3) => {
            if matches!(arg(0), Value::Null) {
                return Some(Value::Null);
            }
            let items = array(arg(0))?;
            let bound = |v: &Value, default: i64| match v {
                Value::Null => Some(default),
                v => int(v),
            };
            let low = bound(arg(1), 1)?.max(1);
            let high = bound(arg(2), items.len() as i64)?.min(items.len() as i64);
            Value::Array(if low > high {
                Vec::new()
            } else {
                items[(low - 1) as usize..high as usize].to_vec()
            })
        }

        // ---- Arrays -------------------------------------------------------------------
        "ARRAY_LENGTH" if arity(2) => {
            let items = array(arg(0))?;
            let dim = int(arg(1))?;
            match dimension_lengths(&items).get((dim - 1).max(0) as usize) {
                Some(&len) if dim >= 1 && len > 0 => Value::Int(len as i64),
                _ => Value::Null,
            }
        }
        "ARRAY_UPPER" if arity(2) => {
            let items = array(arg(0))?;
            let dim = int(arg(1))?;
            match dimension_lengths(&items).get((dim - 1).max(0) as usize) {
                Some(&len) if dim >= 1 && len > 0 => Value::Int(len as i64),
                _ => Value::Null,
            }
        }
        "ARRAY_LOWER" if arity(2) => {
            let items = array(arg(0))?;
            let dim = int(arg(1))?;
            match dimension_lengths(&items).get((dim - 1).max(0) as usize) {
                Some(&len) if dim >= 1 && len > 0 => Value::Int(1),
                _ => Value::Null,
            }
        }
        "ARRAY_NDIMS" if arity(1) => {
            let dims = dimension_lengths(&array(arg(0))?);
            if dims.first().is_some_and(|&n| n > 0) {
                Value::Int(dims.len() as i64)
            } else {
                Value::Null
            }
        }
        "CARDINALITY" if arity(1) => {
            let mut flat = Vec::new();
            flatten(array(arg(0))?, &mut flat);
            Value::Int(flat.len() as i64)
        }
        "ARRAY_APPEND" if arity(2) => {
            let mut items = if matches!(arg(0), Value::Null) {
                Vec::new()
            } else {
                array(arg(0))?
            };
            items.push(arg(1).clone());
            Value::Array(items)
        }
        "ARRAY_PREPEND" if arity(2) => {
            let mut items = if matches!(arg(1), Value::Null) {
                Vec::new()
            } else {
                array(arg(1))?
            };
            items.insert(0, arg(0).clone());
            Value::Array(items)
        }
        "ARRAY_CAT" if arity(2) => match (arg(0), arg(1)) {
            (Value::Null, Value::Null) => Value::Null,
            (Value::Null, b) => Value::Array(array(b)?),
            (a, Value::Null) => Value::Array(array(a)?),
            (a, b) => {
                let mut items = array(a)?;
                items.extend(array(b)?);
                Value::Array(items)
            }
        },
        "ARRAY_POSITION" | "ARRAY_POSITIONS" if arity(2) => {
            if matches!(arg(0), Value::Null) {
                return Some(Value::Null);
            }
            let positions: Vec<i64> = array(arg(0))?
                .iter()
                .enumerate()
                .filter(|(_, v)| match (v, arg(1)) {
                    (Value::Null, Value::Null) => true,
                    (Value::Null, _) | (_, Value::Null) => false,
                    (v, target) => {
                        crate::planner::apply_binary_op(
                            crate::ScalarBinaryOp::Eq,
                            (*v).clone(),
                            target.clone(),
                        ) == Value::Bool(true)
                    }
                })
                .map(|(i, _)| i as i64 + 1)
                .collect();
            if name == "ARRAY_POSITION" {
                positions.first().map_or(Value::Null, |&p| Value::Int(p))
            } else {
                Value::Array(positions.into_iter().map(Value::Int).collect())
            }
        }
        "ARRAY_REMOVE" | "ARRAY_REPLACE" if arity(2) || arity(3) => {
            if matches!(arg(0), Value::Null) {
                return Some(Value::Null);
            }
            let target = arg(1);
            let is_target = |v: &Value| match (v, target) {
                (Value::Null, Value::Null) => true,
                (Value::Null, _) | (_, Value::Null) => false,
                (v, t) => values_equal(v, t),
            };
            let items = array(arg(0))?;
            Value::Array(if name == "ARRAY_REMOVE" {
                items.into_iter().filter(|v| !is_target(v)).collect()
            } else {
                items
                    .into_iter()
                    .map(|v| if is_target(&v) { arg(2).clone() } else { v })
                    .collect()
            })
        }
        "TRIM_ARRAY" if arity(2) => {
            let mut items = array(arg(0))?;
            let n = int(arg(1))?;
            if n < 0 || n as usize > items.len() {
                return Some(raise(
                    "number of elements to trim must be between 0 and the array length",
                ));
            }
            items.truncate(items.len() - n as usize);
            Value::Array(items)
        }
        "ARRAY_SORT" if (1..=3).contains(&args.len()) => {
            let mut items = array(arg(0))?;
            let descending = matches!(args.get(1), Some(Value::Bool(true)));
            let nulls_first = match args.get(2) {
                Some(Value::Bool(b)) => *b,
                _ => descending,
            };
            items.sort_by(|a, b| match (a, b) {
                (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
                (Value::Null, _) => {
                    if nulls_first {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    }
                }
                (_, Value::Null) => {
                    if nulls_first {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                }
                (a, b) => {
                    let ord = crate::value::compare(a, b);
                    if descending { ord.reverse() } else { ord }
                }
            });
            Value::Array(items)
        }
        "ARRAY_REVERSE" if arity(1) => {
            let mut items = array(arg(0))?;
            items.reverse();
            Value::Array(items)
        }
        _ => return None,
    })
}

/// A session value that is only defined while a statement runs.
fn session_unavailable(name: &str) -> Value {
    raise(format!(
        "{}() cannot be evaluated here",
        name.to_ascii_lowercase()
    ))
}

/// Runs a sequence operation for the statement's session; its errors fail
/// the statement.
fn sequence_op(
    op: impl FnOnce(&crate::sequences::SequenceStore, &str) -> anyhow::Result<i64>,
) -> Value {
    let env = session_env::with(|e| {
        e.and_then(|e| e.sequences.clone().map(|s| (s, e.session_id.clone())))
    });
    match env {
        Some((store, session)) => match op(&store, &session) {
            Ok(value) => Value::Int(value),
            Err(e) => raise(e.to_string()),
        },
        None => raise("sequence functions cannot be evaluated here"),
    }
}

fn session_time(pick: impl Fn(&session_env::SessionEnv) -> i64) -> Option<i64> {
    Some(session_env::with(|e| e.map(&pick)).unwrap_or_else(session_env::wall_micros))
}

/// Renders microseconds since the epoch as PostgreSQL timestamp text in UTC.
fn timestamp(micros: i64, with_zone: bool) -> Value {
    match chrono::DateTime::from_timestamp_micros(micros) {
        Some(dt) => Value::Text(crate::value::format_timestamp(dt.naive_utc(), with_zone)),
        None => raise("timestamp out of range"),
    }
}

/// `format_type(oid, typmod)`: the SQL name of a type, with its modifier.
/// A type's name by OID, as `regtype` prints it.
pub(crate) fn format_type_name(oid: i64) -> String {
    format_type(oid, None)
}

fn format_type(oid: i64, typmod: Option<i64>) -> String {
    let base = match oid {
        16 => "boolean",
        17 => "bytea",
        18 => "\"char\"",
        19 => "name",
        20 => "bigint",
        21 => "smallint",
        23 => "integer",
        24 => "regproc",
        25 => "text",
        26 => "oid",
        114 => "json",
        700 => "real",
        701 => "double precision",
        1042 => match typmod {
            Some(m) => return format!("character({})", m - 4),
            None => "bpchar",
        },
        1043 => match typmod {
            Some(m) => return format!("character varying({})", m - 4),
            None => "character varying",
        },
        1082 => "date",
        1083 => "time without time zone",
        1114 => "timestamp without time zone",
        1184 => "timestamp with time zone",
        1186 => "interval",
        1266 => "time with time zone",
        1700 => match typmod {
            Some(m) => {
                let m = m - 4;
                return format!("numeric({},{})", (m >> 16) & 0xffff, m & 0xffff);
            }
            None => "numeric",
        },
        2205 => "regclass",
        2206 => "regtype",
        2950 => "uuid",
        3802 => "jsonb",
        4089 => "regnamespace",
        4096 => "regrole",
        1000 => "boolean[]",
        1005 => "smallint[]",
        1007 => "integer[]",
        1009 => "text[]",
        1014 => "bpchar[]",
        1015 => "character varying[]",
        1016 => "bigint[]",
        1021 => "real[]",
        1022 => "double precision[]",
        1028 => "oid[]",
        1115 => "timestamp without time zone[]",
        1182 => "date[]",
        1185 => "timestamp with time zone[]",
        1231 => "numeric[]",
        199 => "json[]",
        2951 => "uuid[]",
        3807 => "jsonb[]",
        _ => "???",
    };
    base.to_string()
}

fn search_path() -> Vec<String> {
    session_env::setting("search_path")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty() && s != "$user")
        .collect()
}

fn flatten(items: Vec<Value>, out: &mut Vec<Value>) {
    for item in items {
        match item {
            Value::Array(inner) => flatten(inner, out),
            v => out.push(v),
        }
    }
}

/// Lengths of each dimension of a (rectangular) nested array.
fn dimension_lengths(items: &[Value]) -> Vec<usize> {
    let mut dims = vec![items.len()];
    if let Some(Value::Array(inner)) = items.first() {
        dims.extend(dimension_lengths(inner));
    }
    dims
}

fn quote_ident(s: &str) -> String {
    let plain = s
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if plain {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('"', "\"\""))
    }
}

fn quote_literal(s: &str) -> String {
    if s.contains('\\') {
        format!("E'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
    } else {
        format!("'{}'", s.replace('\'', "''"))
    }
}

/// `format()` with `%s`, `%I`, `%L`, `%%`, `n$` positions, and `-`/width.
fn format_text(fmt: &str, args: &[Value]) -> Value {
    let mut out = String::new();
    let mut chars = fmt.chars().peekable();
    let mut next_arg = 0usize;
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        if chars.peek() == Some(&'%') {
            chars.next();
            out.push('%');
            continue;
        }
        let mut spec = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() || c == '$' || c == '-' {
                spec.push(c);
                chars.next();
            } else {
                break;
            }
        }
        let Some(kind) = chars.next() else {
            return raise("unterminated format() type specifier");
        };
        let (position, flags) = match spec.split_once('$') {
            Some((pos, rest)) => match pos.parse::<usize>() {
                Ok(p) if p >= 1 => (p - 1, rest.to_string()),
                _ => {
                    return raise("format specifies argument 0, but arguments are numbered from 1");
                }
            },
            None => (next_arg, spec),
        };
        next_arg = position + 1;
        let Some(value) = args.get(position) else {
            return raise("too few arguments for format()");
        };
        let rendered = match kind {
            's' => match value {
                Value::Null => String::new(),
                v => text(v),
            },
            'I' => match value {
                Value::Null => {
                    return raise("null values cannot be formatted as an SQL identifier");
                }
                v => quote_ident(&text(v)),
            },
            'L' => match value {
                Value::Null => "NULL".to_string(),
                v => quote_literal(&text(v)),
            },
            other => return raise(format!("unrecognized format() type specifier \"{other}\"")),
        };
        let left = flags.starts_with('-');
        let width: usize = flags.trim_start_matches('-').parse().unwrap_or(0);
        let pad = width.saturating_sub(rendered.chars().count());
        if left {
            out.push_str(&rendered);
            out.push_str(&" ".repeat(pad));
        } else {
            out.push_str(&" ".repeat(pad));
            out.push_str(&rendered);
        }
    }
    Value::Text(out)
}

/// Compiles a POSIX-style pattern with PostgreSQL flags (`i` case-insensitive,
/// `g` handled by the caller, `n`/`m`/`s` line modes).
fn regex_with_flags(pattern: &str, flags: &str) -> Option<regex::Regex> {
    let mut prefix = String::new();
    if flags.contains('i') {
        prefix.push('i');
    }
    if flags.contains('n') || flags.contains('m') {
        prefix.push('m');
    }
    let source = if prefix.is_empty() {
        pattern.to_string()
    } else {
        format!("(?{prefix}){pattern}")
    };
    regex::Regex::new(&source).ok()
}

/// Rewrites a PostgreSQL replacement string (`\1`, `\&`) for the regex crate.
fn pg_replacement(replacement: &str) -> String {
    let mut out = String::new();
    let mut chars = replacement.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '$' => out.push_str("$$"),
            '\\' => match chars.next() {
                Some(d) if d.is_ascii_digit() => {
                    out.push_str(&format!("${{{d}}}"));
                }
                Some('&') => out.push_str("${0}"),
                Some(other) => out.push(other),
                None => out.push('\\'),
            },
            c => out.push(c),
        }
    }
    out
}

/// A value as JSON, as `to_jsonb` renders it.
pub(crate) fn to_json(v: &Value) -> serde_json::Value {
    use serde_json::Value as J;
    match v {
        Value::Null => J::Null,
        Value::Int(i) => J::from(*i),
        Value::Float(f) => serde_json::Number::from_f64(*f).map_or(J::Null, J::Number),
        // Exactly, as `numeric` prints it (`1.50`).
        Value::Numeric(d) => d
            .to_string()
            .parse::<serde_json::Number>()
            .map_or(J::Null, J::Number),
        Value::Bool(b) => J::Bool(*b),
        Value::Text(s) => J::String(s.clone()),
        Value::Array(items) => J::Array(items.iter().map(to_json).collect()),
        Value::Jsonb(j) => j.clone(),
    }
}

/// A `json`/`jsonb` argument as a document: text is JSON input (so `'1'` is
/// the number 1, as an untyped literal would be).
fn json_arg(v: &Value) -> Option<serde_json::Value> {
    match v {
        Value::Text(s) => crate::json_text::parse(s)
            .ok()
            .or_else(|| crate::filter_eval::value_to_json(v)),
        other => crate::filter_eval::value_to_json(other),
    }
}

/// A JSON argument given as text (`'{"a":1}'`) is parsed; other values stay.
fn parse_json_arg(v: &Value) -> Value {
    match v {
        Value::Text(s) => serde_json::from_str(s).map_or_else(|_| v.clone(), Value::Jsonb),
        other => other.clone(),
    }
}

fn json_set(
    json: &mut serde_json::Value,
    path: &[String],
    new_value: serde_json::Value,
    create: bool,
) {
    let Some((key, rest)) = path.split_first() else {
        return;
    };
    match json {
        serde_json::Value::Object(map) => {
            if rest.is_empty() {
                if create || map.contains_key(key) {
                    map.insert(key.clone(), new_value);
                }
            } else if let Some(child) = map.get_mut(key) {
                json_set(child, rest, new_value, create);
            }
        }
        serde_json::Value::Array(items) => {
            let Ok(idx) = key.parse::<i64>() else {
                return;
            };
            let len = items.len() as i64;
            let pos = if idx < 0 { len + idx } else { idx };
            if rest.is_empty() {
                if (0..len).contains(&pos) {
                    items[pos as usize] = new_value;
                } else if create {
                    if pos >= len {
                        items.push(new_value);
                    } else {
                        items.insert(0, new_value);
                    }
                }
            } else if (0..len).contains(&pos) {
                json_set(&mut items[pos as usize], rest, new_value, create);
            }
        }
        _ => {}
    }
}

fn strip_nulls(json: &mut serde_json::Value, in_arrays: bool) {
    match json {
        serde_json::Value::Object(map) => {
            map.retain(|_, v| !v.is_null());
            map.values_mut().for_each(|v| strip_nulls(v, in_arrays));
        }
        serde_json::Value::Array(items) => {
            if in_arrays {
                items.retain(|v| !v.is_null());
            }
            items.iter_mut().for_each(|v| strip_nulls(v, in_arrays));
        }
        _ => {}
    }
}

fn size_pretty(bytes: i64) -> String {
    const UNITS: [&str; 5] = ["bytes", "kB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    // PostgreSQL switches units once a value reaches 10240 of the current unit.
    while value.abs() >= 10_240.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{} {}", value.round() as i64, UNITS[unit])
}

/// A version-7 UUID for `ms` milliseconds since the epoch: time-ordered, with
/// the remaining bits random (RFC 9562).
fn uuid_v7(ms: i64) -> uuid::Uuid {
    let random = uuid::Uuid::new_v4().into_bytes();
    let mut bytes = [0u8; 16];
    bytes[..6].copy_from_slice(&(ms as u64).to_be_bytes()[2..]);
    bytes[6..].copy_from_slice(&random[6..]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
}

/// A uniformly distributed value in `[0, 1)`.
fn random_unit() -> f64 {
    let bits = u64::from_le_bytes(
        uuid::Uuid::new_v4().as_bytes()[..8]
            .try_into()
            .unwrap_or([0; 8]),
    );
    (bits >> 11) as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> Value {
        Value::Text(s.into())
    }

    #[test]
    fn strings_follow_postgres_semantics() {
        assert_eq!(
            call("SUBSTR", &[t("hello"), Value::Int(0), Value::Int(2)]),
            t("h")
        );
        assert_eq!(call("SUBSTR", &[t("hello"), Value::Int(2)]), t("ello"));
        assert_eq!(
            call("SPLIT_PART", &[t("a,b,c"), t(","), Value::Int(-1)]),
            t("c")
        );
        assert_eq!(call("LEFT", &[t("hello"), Value::Int(-1)]), t("hell"));
        assert_eq!(call("LPAD", &[t("toolong"), Value::Int(3)]), t("too"));
        assert_eq!(call("INITCAP", &[t("hello wORLD")]), t("Hello World"));
        assert_eq!(call("TRANSLATE", &[t("abc"), t("ab"), t("x")]), t("xc"));
        assert_eq!(
            call(
                "FORMAT",
                &[
                    t("%s|%I|%L|[%-4s]"),
                    t("x"),
                    t("My Col"),
                    t("it's"),
                    t("ab")
                ]
            ),
            t("x|\"My Col\"|'it''s'|[ab  ]")
        );
        assert_eq!(
            call(
                "REGEXP_REPLACE",
                &[t("a1b22c"), t("[0-9]+"), t("#"), t("g")]
            ),
            t("a#b#c")
        );
        assert_eq!(
            call("CONCAT_WS", &[t("-"), t("a"), Value::Null, t("b")]),
            t("a-b")
        );
        assert_eq!(call("UPPER", &[Value::Null]), Value::Null);
    }

    #[test]
    fn math_domain_errors_fail_the_statement() {
        crate::eval_error::reset();
        assert_eq!(call("SQRT", &[Value::Int(16)]), Value::Float(4.0));
        call("SQRT", &[Value::Int(-1)]);
        assert!(crate::eval_error::check().is_err());
        call("LN", &[Value::Int(0)]);
        assert!(crate::eval_error::check().is_err());
        assert_eq!(
            call("GCD", &[Value::Int(12), Value::Int(18)]),
            Value::Int(6)
        );
        assert_eq!(call("LCM", &[Value::Int(4), Value::Int(6)]), Value::Int(12));
        assert_eq!(call("CEIL", &[Value::Float(4.2)]), Value::Float(5.0));
        assert_eq!(
            call(
                "WIDTH_BUCKET",
                &[
                    Value::Float(5.35),
                    Value::Float(0.024),
                    Value::Float(10.06),
                    Value::Int(5)
                ]
            ),
            Value::Int(3)
        );
        assert!(crate::eval_error::check().is_ok());
    }

    #[test]
    fn unknown_functions_and_wrong_arity_fail() {
        crate::eval_error::reset();
        call("NO_SUCH_FUNCTION", &[Value::Int(1)]);
        assert!(crate::eval_error::check().is_err());
        call("UPPER", &[t("a"), t("b")]);
        assert!(crate::eval_error::check().is_err());
    }

    #[test]
    fn arrays_and_json() {
        let arr = Value::Array(vec![Value::Int(3), Value::Int(1), Value::Int(2)]);
        assert_eq!(
            call("ARRAY_SORT", std::slice::from_ref(&arr)),
            Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
        );
        assert_eq!(
            call("ARRAY_POSITION", &[arr.clone(), Value::Int(2)]),
            Value::Int(3)
        );
        assert_eq!(call("CARDINALITY", &[arr]), Value::Int(3));
        assert_eq!(
            call("JSONB_BUILD_OBJECT", &[t("a"), Value::Int(1)]),
            Value::Jsonb(serde_json::json!({"a": 1}))
        );
        assert_eq!(call("JSONB_TYPEOF", &[t("[]")]), t("array"));
        let v7 = call("UUIDV7", &[]);
        assert_eq!(call("UUID_EXTRACT_VERSION", &[v7]), Value::Int(7));
    }
}
