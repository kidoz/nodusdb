//! PostgreSQL's SQL/JSON functions (SQL:2023, PostgreSQL 16+): the
//! `IS [NOT] JSON` predicates, over the [`crate::jsonpath`] engine.
/// The call `<expr> IS [NOT] JSON [VALUE|SCALAR|ARRAY|OBJECT]
/// [WITH|WITHOUT UNIQUE [KEYS]]` is lowered to:
/// `__IS_JSON__(value, kind, unique, negated)`.
pub(crate) const IS_JSON: &str = "__IS_JSON__";

/// A call the planner reports before execution, as PostgreSQL words it:
/// `__SQL_JSON_ERROR__(message, sqlstate)`.
pub(crate) const SQL_JSON_ERROR: &str = "__SQL_JSON_ERROR__";

use serde_json::Value as J;

/// An error as PostgreSQL words it: the message with its SQLSTATE.
fn error(message: impl Into<String>, code: &str) -> String {
    crate::error_fields::DbError::new(message)
        .code(code)
        .into_text()
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
