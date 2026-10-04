//! PostgreSQL's SQL/JSON functions (SQL:2023, PostgreSQL 16+): the
//! `IS [NOT] JSON` predicates, the `JSON_*` constructors, and the
//! `JSON_EXISTS`/`JSON_VALUE`/`JSON_QUERY` query functions. The path engine
//! is [`crate::jsonpath`]'s; this module holds the conversion rules around it.

/// The call `<expr> IS [NOT] JSON [VALUE|SCALAR|ARRAY|OBJECT]
/// [WITH|WITHOUT UNIQUE [KEYS]]` is lowered to:
/// `__IS_JSON__(value, kind, unique, negated)`.
pub(crate) const IS_JSON: &str = "__IS_JSON__";

/// A call the planner reports before execution, as PostgreSQL words it:
/// `__SQL_JSON_ERROR__(message, sqlstate)`.
pub(crate) const SQL_JSON_ERROR: &str = "__SQL_JSON_ERROR__";

/// The SQL/JSON query functions, as the parser rewrites them:
/// `__JSON_EXISTS__(doc, format, path, vars, on_error)`,
/// `__JSON_VALUE__(doc, format, path, vars, returning, retformat, on_empty,
/// on_empty_default, on_error, on_error_default)`, and
/// `__JSON_QUERY__(doc, format, path, vars, returning, retformat, wrapper,
/// quotes, on_empty, on_empty_default, on_error, on_error_default)`.
pub(crate) const JSON_EXISTS: &str = "__JSON_EXISTS__";
pub(crate) const JSON_VALUE: &str = "__JSON_VALUE__";
pub(crate) const JSON_QUERY: &str = "__JSON_QUERY__";

/// The call a `PASSING` clause becomes: `__JSON_VARS__('name', value, ...)`,
/// the variables object PostgreSQL hands the path evaluator.
pub(crate) const JSON_VARS: &str = "__JSON_VARS__";

/// The JSON value constructors: `__JSON__(value, unique-keys)`,
/// `__JSON_SCALAR__(value)`, and
/// `__JSON_SERIALIZE__(value, format, returning-type)`.
pub(crate) const JSON: &str = "__JSON__";
pub(crate) const JSON_SCALAR: &str = "__JSON_SCALAR__";
pub(crate) const JSON_SERIALIZE: &str = "__JSON_SERIALIZE__";

/// `JSON(expr [WITH UNIQUE KEYS])`: the value as a `json`, its text kept as
/// written.
pub(crate) fn json(value: &crate::Value, unique: &str) -> Result<crate::Value, String> {
    let text = match value {
        crate::Value::Jsonb(value) => crate::json_text::jsonb_text(value),
        crate::Value::Json(text) => text.clone(),
        crate::Value::Text(text) => {
            crate::json_text::parse(text)?;
            text.clone()
        }
        other => {
            return Err(error(
                format!(
                    "cannot cast type {} to json",
                    crate::value::value_type_name(other)
                ),
                "42846",
            ));
        }
    };
    if unique == "unique" && has_duplicate_key(&text) {
        return Err(error("duplicate JSON object key value", "22030"));
    }
    Ok(crate::Value::Json(text))
}

/// `JSON_SCALAR(expr)`: the value as a `json`. A `json` or `jsonb` value is
/// kept (a text value cast to one is JSON already), and any other value is
/// written as `to_json` would write it. The planner names the JSON types,
/// since a `json` value is also text at run time.
pub(crate) fn json_scalar(value: &crate::Value, is_json: bool) -> crate::Value {
    match value {
        crate::Value::Json(text) => crate::Value::Json(text.clone()),
        crate::Value::Jsonb(value) => crate::Value::Json(crate::json_text::jsonb_text(value)),
        crate::Value::Text(text) if is_json => crate::Value::Json(text.clone()),
        other => {
            let mut out = String::new();
            crate::json_text::value_json(other, false, &mut out);
            crate::Value::Json(out)
        }
    }
}

/// `JSON_SERIALIZE(expr [FORMAT JSON] [RETURNING type])`: the JSON text of
/// the value, as the RETURNING type (`text` by default).
pub(crate) fn json_serialize(
    value: &crate::Value,
    returning: &str,
) -> Result<crate::Value, String> {
    let text = match value {
        crate::Value::Jsonb(value) => crate::json_text::jsonb_text(value),
        crate::Value::Json(text) => text.clone(),
        crate::Value::Text(text) => text.clone(),
        crate::Value::Bytea(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => {
            return Err(error(
                format!(
                    "cannot cast type {} to json",
                    crate::value::value_type_name(other)
                ),
                "42846",
            ));
        }
    };
    if crate::value::is_bytea_type(returning) {
        return Ok(crate::Value::Bytea(text.into_bytes()));
    }
    crate::value::fit_character(&text, returning, false).map(crate::Value::Text)
}

use serde_json::Value as J;

/// An error as PostgreSQL words it: the message with its SQLSTATE.
fn error(message: impl Into<String>, code: &str) -> String {
    crate::error_fields::DbError::new(message)
        .code(code)
        .into_text()
}

/// The document a SQL/JSON function reads, as a JSON value. The planner casts
/// the document to `jsonb`, so a value here is one already.
pub(crate) fn document(value: &crate::Value) -> Result<J, String> {
    match value {
        crate::Value::Jsonb(value) => Ok(value.clone()),
        crate::Value::Json(text) | crate::Value::Text(text) => crate::json_text::parse(text),
        other => Err(error(
            format!(
                "cannot cast type {} to jsonb",
                crate::value::value_type_name(other)
            ),
            "42846",
        )),
    }
}

/// The variables a `PASSING` clause names, as the path evaluator's object.
pub(crate) fn vars(value: &crate::Value) -> Result<J, String> {
    match value {
        crate::Value::Null => Ok(J::Object(serde_json::Map::new())),
        crate::Value::Jsonb(value) => Ok(value.clone()),
        crate::Value::Text(text) => crate::json_text::parse(text),
        other => Err(error(
            format!(
                "cannot cast type {} to jsonb",
                crate::value::value_type_name(other)
            ),
            "42846",
        )),
    }
}

/// The item as JSON text, in the canonical spelling.
fn item_text(item: &J) -> String {
    crate::json_text::jsonb_text(item)
}

/// The item as the scalar text a character or numeric RETURNING type takes:
/// a string without its quotes, everything else as its JSON text.
fn scalar_text(item: &J) -> String {
    match item {
        J::String(text) => text.clone(),
        other => item_text(other),
    }
}

/// A behavior clause as the parser encoded it.
fn raises(behavior: &str) -> bool {
    behavior == "error"
}

/// `JSON_EXISTS`: whether the path produces an item. An error the path raises
/// is the ON ERROR clause's to handle (its default is FALSE).
pub(crate) fn json_exists(
    value: &crate::Value,
    path: &str,
    vars: Option<&crate::Value>,
    on_error: &str,
) -> Result<crate::Value, String> {
    let doc = document(value)?;
    let vars = match vars {
        Some(vars) => Some(self::vars(vars)?),
        None => None,
    };
    let found = crate::jsonpath::exists(&doc, path, vars.as_ref(), false);
    let result = match found {
        Ok(Some(found)) => found,
        // A failure the ON ERROR clause handles; there is no ON EMPTY for
        // this function, so an empty result is false, as PostgreSQL has it.
        Ok(None) => false,
        Err(text) => match on_error {
            "true" => true,
            "unknown" => return Ok(crate::Value::Null),
            "error" => return Err(text),
            _ => false,
        },
    };
    Ok(crate::Value::Bool(result))
}

/// `JSON_VALUE`: the single scalar item the path produces, as the RETURNING
/// type.
pub(crate) fn json_value(
    value: &crate::Value,
    path: &str,
    vars: Option<&crate::Value>,
    returning: &str,
    on_empty: &str,
    on_empty_default: &crate::Value,
    on_error: &str,
    on_error_default: &crate::Value,
) -> Result<crate::Value, String> {
    let doc = document(value)?;
    let vars = match vars {
        Some(vars) => Some(self::vars(vars)?),
        None => None,
    };
    let items = crate::jsonpath::execute(path, &doc, vars.as_ref(), false);
    let items = match items {
        Ok(items) => items,
        Err(text) => {
            return behavior(on_error, on_error_default, || Err(text));
        }
    };
    match items.len() {
        0 => behavior(on_empty, on_empty_default, || {
            Err(error("no SQL/JSON item found for specified path", "22035"))
        }),
        1 => {
            let item = &items[0];
            // A JSON null is SQL NULL, whatever the RETURNING type.
            if item.is_null() {
                return Ok(crate::Value::Null);
            }
            if item.is_array() || item.is_object() {
                return behavior(on_error, on_error_default, || {
                    Err(error(
                        "JSON path expression in JSON_VALUE must return single scalar item",
                        "2203F",
                    ))
                });
            }
            match convert(item, returning) {
                Ok(value) => Ok(value),
                Err(text) => behavior(on_error, on_error_default, || Err(text)),
            }
        }
        _ => behavior(on_error, on_error_default, || {
            Err(error(
                "JSON path expression in JSON_VALUE must return single scalar item",
                "22034",
            ))
        }),
    }
}

/// `JSON_QUERY`: the path's items, wrapped as the clause asks, as the
/// RETURNING type.
#[allow(clippy::too_many_arguments)]
pub(crate) fn json_query(
    value: &crate::Value,
    path: &str,
    vars: Option<&crate::Value>,
    returning: &str,
    wrapper: &str,
    quotes: &str,
    on_empty: &str,
    on_empty_default: &crate::Value,
    on_error: &str,
    on_error_default: &crate::Value,
) -> Result<crate::Value, String> {
    let doc = document(value)?;
    let vars = match vars {
        Some(vars) => Some(self::vars(vars)?),
        None => None,
    };
    let items = crate::jsonpath::execute(path, &doc, vars.as_ref(), false);
    let items = match items {
        Ok(items) => items,
        Err(text) => {
            return behavior(on_error, on_error_default, || Err(text));
        }
    };
    if items.is_empty() {
        return behavior(on_empty, on_empty_default, || {
            Err(error("no SQL/JSON item found for specified path", "22035"))
        });
    }
    let wrapped = wrapper == "with" || (wrapper == "conditional" && items.len() > 1);
    if !wrapped && items.len() > 1 {
        return behavior(on_error, on_error_default, || {
            Err(error(
                "JSON path expression in JSON_QUERY must return single item when no wrapper \
                 is requested",
                "22034",
            ))
        });
    }
    let result = if wrapped {
        J::Array(items)
    } else {
        items.into_iter().next().expect("checked above")
    };
    // A character type takes the item's JSON text, with its quotes, unless
    // `OMIT QUOTES` unwraps a scalar string.
    if is_text_target(returning) {
        let text = if !wrapped && quotes == "omit" && matches!(result, J::String(_)) {
            scalar_text(&result)
        } else {
            item_text(&result)
        };
        return crate::value::fit_character(&text, returning, false).map(crate::Value::Text);
    }
    convert(&result, returning)
}

/// The value a behavior clause yields when its case arises: the DEFAULT
/// expression, NULL, an empty array or object, or the error itself.
fn behavior(
    kind: &str,
    default: &crate::Value,
    error: impl FnOnce() -> Result<crate::Value, String>,
) -> Result<crate::Value, String> {
    match kind {
        "default" => Ok(default.clone()),
        "error" => error(),
        "empty-array" => Ok(crate::Value::Jsonb(J::Array(Vec::new()))),
        "empty-object" => Ok(crate::Value::Jsonb(J::Object(serde_json::Map::new()))),
        // NULL, and the default where a clause was not given.
        _ => Ok(crate::Value::Null),
    }
}

/// Whether a RETURNING type is a character type (where quotes may be
/// omitted).
fn is_text_target(returning: &str) -> bool {
    let upper = returning.trim().to_ascii_uppercase();
    let base = upper.split('(').next().unwrap_or_default().trim();
    matches!(
        base,
        "TEXT" | "VARCHAR" | "CHARACTER VARYING" | "CHAR" | "CHARACTER" | "BPCHAR"
    )
}

/// An item as the RETURNING type: a character type takes the scalar text, a
/// numeric or temporal one its input syntax, and `json`/`jsonb` the item's
/// JSON text.
fn convert(item: &J, returning: &str) -> Result<crate::Value, String> {
    let upper = returning.trim().to_ascii_uppercase();
    let base = upper.split('(').next().unwrap_or_default().trim();
    if matches!(base, "JSON" | "JSONB") {
        return Ok(match base {
            "JSON" => crate::Value::Json(item_text(item)),
            _ => crate::Value::Jsonb(item.clone()),
        });
    }
    let text = scalar_text(item);
    if is_text_target(returning) || base.is_empty() {
        return crate::value::fit_character(&text, returning, false).map(crate::Value::Text);
    }
    crate::planner::try_cast(crate::Value::Text(text), returning)
}

/// Whether a value is a JSON value of the predicate's kind. An invalid text
/// document is not an error here: `'abc' IS JSON` is false, as PostgreSQL
/// decides (the error is raised by the jsonpath functions instead).
pub(crate) fn is_json(value: &crate::Value, kind: &str, unique: &str, negated: bool) -> bool {
    let parsed = match value {
        crate::Value::Jsonb(value) => Some(value.clone()),
        crate::Value::Json(text) | crate::Value::Text(text) => crate::json_text::parse(text).ok(),
        _ => None,
    };
    let Some(parsed) = parsed else {
        return negated;
    };
    let shape = match kind {
        "scalar" => !parsed.is_array() && !parsed.is_object(),
        "array" => parsed.is_array(),
        "object" => parsed.is_object(),
        _ => true,
    };
    // A `jsonb` value has no duplicate keys left; a `json` one keeps them in
    // its text, where the check reads them.
    let keys_unique = match unique {
        "unique" => match value {
            crate::Value::Jsonb(_) => true,
            crate::Value::Json(text) | crate::Value::Text(text) => !has_duplicate_key(text),
            _ => true,
        },
        _ => true,
    };
    let result = shape && keys_unique;
    if negated { !result } else { result }
}

/// Whether a JSON text has a duplicate object key, as `WITH UNIQUE KEYS`
/// asks. The text is valid (checked before by [`crate::json_text::parse`]), so
/// a scan that loses its way reports no duplicates.
pub(crate) fn has_duplicate_key(text: &str) -> bool {
    let mut scan = KeyScan {
        chars: text.chars().collect(),
        pos: 0,
    };
    scan.value(0) == Some(true)
}

/// A scanner over a JSON text that tracks each object's keys.
struct KeyScan {
    chars: Vec<char>,
    pos: usize,
}

impl KeyScan {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(' ' | '\t' | '\n' | '\r')) {
            self.pos += 1;
        }
    }

    /// Scans one value; `Some(true)` when a duplicate key was seen.
    fn value(&mut self, depth: usize) -> Option<bool> {
        if depth > 1000 {
            return None;
        }
        self.skip_ws();
        match self.peek()? {
            '{' => {
                self.pos += 1;
                let mut keys: Vec<String> = Vec::new();
                loop {
                    self.skip_ws();
                    if self.peek() == Some('}') {
                        self.pos += 1;
                        return Some(false);
                    }
                    let key = self.string()?;
                    if keys.iter().any(|k| *k == key) {
                        return Some(true);
                    }
                    keys.push(key);
                    self.skip_ws();
                    if self.peek() != Some(':') {
                        return None;
                    }
                    self.pos += 1;
                    if self.value(depth + 1)? {
                        return Some(true);
                    }
                    self.skip_ws();
                    match self.peek() {
                        Some(',') => self.pos += 1,
                        Some('}') => {
                            self.pos += 1;
                            return Some(false);
                        }
                        _ => return None,
                    }
                }
            }
            '[' => {
                self.pos += 1;
                loop {
                    self.skip_ws();
                    if self.peek() == Some(']') {
                        self.pos += 1;
                        return Some(false);
                    }
                    if self.value(depth + 1)? {
                        return Some(true);
                    }
                    self.skip_ws();
                    match self.peek() {
                        Some(',') => self.pos += 1,
                        Some(']') => {
                            self.pos += 1;
                            return Some(false);
                        }
                        _ => return None,
                    }
                }
            }
            '"' => {
                self.string()?;
                Some(false)
            }
            _ => {
                // A number, `true`, `false`, or `null`: runs to the next
                // structural character.
                while let Some(c) = self.peek() {
                    if matches!(c, ',' | '}' | ']') {
                        break;
                    }
                    self.pos += 1;
                }
                Some(false)
            }
        }
    }

    /// A JSON string, with its escapes decoded so `"\u0061"` equals `"a"`.
    fn string(&mut self) -> Option<String> {
        if self.peek() != Some('"') {
            return None;
        }
        self.pos += 1;
        let mut out = String::new();
        loop {
            let c = self.peek()?;
            self.pos += 1;
            match c {
                '"' => return Some(out),
                '\\' => {
                    let escape = self.peek()?;
                    self.pos += 1;
                    match escape {
                        '"' => out.push('"'),
                        '\\' => out.push('\\'),
                        '/' => out.push('/'),
                        'b' => out.push('\u{8}'),
                        'f' => out.push('\u{c}'),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' => {
                            let mut code = self.hex4()?;
                            // A surrogate pair names one character.
                            if (0xD800..0xDC00).contains(&code)
                                && self.peek() == Some('\\')
                                && self.chars.get(self.pos + 1) == Some(&'u')
                            {
                                self.pos += 2;
                                let low = self.hex4()?;
                                if (0xDC00..0xE000).contains(&low) {
                                    code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                                }
                            }
                            out.extend(char::from_u32(code).or(Some('\u{FFFD}')));
                        }
                        _ => return None,
                    }
                }
                c => out.push(c),
            }
        }
    }

    fn hex4(&mut self) -> Option<u32> {
        let mut value = 0u32;
        for _ in 0..4 {
            let c = self.peek()?;
            self.pos += 1;
            value = value * 16 + c.to_digit(16)?;
        }
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(text: &str) -> crate::Value {
        crate::Value::Json(text.to_string())
    }

    /// A `jsonb` value, for the query functions' documents.
    fn document_of(text: &str) -> crate::Value {
        crate::Value::Jsonb(crate::json_text::parse(text).expect("valid JSON"))
    }

    fn shown(result: Result<crate::Value, String>) -> String {
        match result {
            Ok(crate::Value::Null) => "<NULL>".to_string(),
            Ok(value) => crate::render(&value),
            Err(error) => format!("error: {error}"),
        }
    }

    #[test]
    fn query_functions_match_postgresql() {
        let exists = |doc: &str, path: &str, on_error: &str| {
            shown(json_exists(&document_of(doc), path, None, on_error))
        };
        assert_eq!(exists("{\"a\":1}", "$.a", ""), "t");
        assert_eq!(exists("{\"a\":1}", "$.b", ""), "f");
        assert_eq!(exists("{\"a\":1}", "strict $.b", ""), "f");
        assert_eq!(exists("{\"a\":1}", "strict $.b", "true"), "t");
        assert_eq!(exists("{\"a\":1}", "strict $.b", "unknown"), "<NULL>");
        assert!(exists("{\"a\":1}", "strict $.b", "error").starts_with("error: "));

        let query = |doc: &str, path: &str, wrapper: &str, quotes: &str, on_empty: &str| {
            shown(json_query(
                &document_of(doc),
                path,
                None,
                "text",
                wrapper,
                quotes,
                on_empty,
                &crate::Value::Null,
                "",
                &crate::Value::Null,
            ))
        };
        assert_eq!(query("{\"a\":1}", "$.a", "", "", ""), "1");
        assert_eq!(query("{\"a\":\"x\"}", "$.a", "", "", ""), "\"x\"");
        assert_eq!(query("{\"a\":\"x\"}", "$.a", "", "omit", ""), "x");
        assert_eq!(query("{\"a\":[1,2]}", "$.a[*]", "with", "", ""), "[1, 2]");
        assert_eq!(
            query("{\"a\":[1,2]}", "$.a[*]", "conditional", "", ""),
            "[1, 2]"
        );
        assert_eq!(query("{\"a\":[1,2]}", "$.a[0]", "conditional", "", ""), "1");
        assert_eq!(query("{\"a\":1}", "$.b", "", "", ""), "<NULL>");
        assert_eq!(query("{\"a\":1}", "$.b", "", "", "empty-object"), "{}");

        let value_of = |doc: &str, path: &str, returning: &str| {
            shown(json_value(
                &document_of(doc),
                path,
                None,
                returning,
                "",
                &crate::Value::Null,
                "",
                &crate::Value::Null,
            ))
        };
        assert_eq!(value_of("{\"a\":1}", "$.a", "text"), "1");
        assert_eq!(value_of("{\"a\":\"x\"}", "$.a", "text"), "x");
        assert_eq!(value_of("{\"a\":\"1\"}", "$.a", "int"), "1");
        assert_eq!(value_of("{\"a\":null}", "$.a", "int"), "<NULL>");
        assert_eq!(value_of("{\"a\":[1]}", "$.a", "int"), "<NULL>");
        assert_eq!(value_of("{\"b\":1}", "$.a", "int"), "<NULL>");
        assert_eq!(value_of("{\"a\":\"x\"}", "$.a", "int"), "<NULL>");
        assert!(
            shown(json_value(
                &document_of("{\"a\":\"x\"}"),
                "$.a",
                None,
                "int",
                "",
                &crate::Value::Null,
                "error",
                &crate::Value::Null,
            ))
            .starts_with("error: ")
        );
        // The DEFAULT clause for ON EMPTY.
        assert_eq!(
            shown(json_value(
                &document_of("{\"b\":1}"),
                "$.a",
                None,
                "int",
                "default",
                &crate::Value::Int(42),
                "",
                &crate::Value::Null,
            )),
            "42"
        );
    }

    #[test]
    fn constructors_match_postgresql() {
        // The `json` the test module defines above is the `json` value
        // helper, so the constructor is named explicitly.
        let constructor = |value: &crate::Value, unique: &str| match super::json(value, unique) {
            Ok(crate::Value::Json(text)) => text,
            Ok(other) => panic!("not json: {other:?}"),
            Err(error) => format!("error: {error}"),
        };
        assert_eq!(
            constructor(&crate::Value::Text("{\"a\":1}".into()), ""),
            "{\"a\":1}"
        );
        assert_eq!(
            constructor(&document_of("{\"a\":1}"), ""),
            "{\"a\": 1}",
            "a jsonb value is written canonically"
        );
        assert!(
            constructor(&crate::Value::Text("{\"a\":1,\"a\":2}".into()), "unique")
                .starts_with("error: ")
        );
        assert_eq!(
            constructor(
                &crate::Value::Text("{\"a\":1,\"a\":2}".into()),
                "not-unique"
            ),
            "{\"a\":1,\"a\":2}"
        );
        assert_eq!(
            shown(Ok(json_scalar(&crate::Value::Text("abc".into()), false))),
            "\"abc\""
        );
        assert_eq!(shown(Ok(json_scalar(&crate::Value::Int(1), false))), "1");
        assert_eq!(
            shown(Ok(json_scalar(
                &crate::Value::Text("{\"a\":1}".into()),
                true
            ))),
            "{\"a\":1}"
        );
        assert_eq!(
            shown(json_serialize(&document_of("{\"a\":1}"), "text")),
            "{\"a\": 1}"
        );
        assert_eq!(
            shown(json_serialize(
                &crate::Value::Json("{\"a\":1}".into()),
                "text"
            )),
            "{\"a\":1}"
        );
        assert!(
            shown(json_serialize(
                &crate::Value::Json("{}".into()),
                "varchar(1)"
            ))
            .starts_with("error: ")
        );
    }

    #[test]
    fn is_json_matches_postgresql() {
        let is =
            |value: &crate::Value, kind: &str, unique: &str| is_json(value, kind, unique, false);
        assert!(is(&json("{\"a\":1}"), "value", "either"));
        assert!(is(&json("1"), "scalar", "either"));
        assert!(is(&json("null"), "scalar", "either"));
        assert!(!is(&json("1"), "array", "either"));
        assert!(is(&json("[1]"), "array", "either"));
        assert!(is(&json("{\"a\":1}"), "object", "either"));
        assert!(!is(&json("{\"a\":1}"), "scalar", "either"));
        assert!(!is(&json("abc"), "value", "either"));
        assert!(!is(&json(""), "value", "either"));
        assert!(!is(&json("{\"a\":1,\"a\":2}"), "object", "unique"));
        assert!(is(&json("{\"a\":1,\"a\":2}"), "object", "not-unique"));
        assert!(is(&json("{\"a\":1}"), "object", "unique"));
        assert!(!is(&json("{\"a\":{\"b\":1,\"b\":2}}"), "object", "unique"));
        assert!(!is(&json("{\"a\":1,\"\\u0061\":2}"), "object", "unique"));
        assert!(is(&json("[{\"a\":1},{\"a\":1}]"), "array", "unique"));
        assert!(is(
            &crate::Value::Jsonb(crate::json_text::parse("{\"a\":1}").unwrap()),
            "object",
            "unique"
        ));
        assert!(is_json(&json("abc"), "value", "either", true));
        assert!(!is_json(&json("{\"a\":1}"), "value", "either", true));
    }
}
