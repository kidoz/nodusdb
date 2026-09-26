//! JSON as PostgreSQL writes and orders it: `jsonb`'s normalized text (keys
//! unique and ordered shorter first, `", "` and `": "` separators, numbers
//! without exponents), the layout of `json` built by functions, and
//! `jsonb_pretty`.

use serde_json::Value as J;
use std::cmp::Ordering;

/// Parses JSON input, with PostgreSQL's error for malformed text.
pub(crate) fn parse(text: &str) -> Result<J, String> {
    serde_json::from_str(text).map_err(|_| "invalid input syntax for type json".to_string())
}

/// `jsonb`'s order of object keys: shorter keys first, then bytewise.
fn key_order(a: &str, b: &str) -> Ordering {
    a.len()
        .cmp(&b.len())
        .then_with(|| a.as_bytes().cmp(b.as_bytes()))
}

/// An object's entries in `jsonb` key order.
fn sorted_entries(map: &serde_json::Map<String, J>) -> Vec<(&String, &J)> {
    let mut entries: Vec<_> = map.iter().collect();
    entries.sort_by(|a, b| key_order(a.0, b.0));
    entries
}

/// A value as `jsonb` text: `{"a": 1, "b": [1, 2]}`.
pub fn jsonb_text(value: &J) -> String {
    let mut out = String::new();
    write_jsonb(value, &mut out);
    out
}

fn write_jsonb(value: &J, out: &mut String) {
    match value {
        J::Object(map) => {
            out.push('{');
            for (i, (key, item)) in sorted_entries(map).into_iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_string(key, out);
                out.push_str(": ");
                write_jsonb(item, out);
            }
            out.push('}');
        }
        J::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_jsonb(item, out);
            }
            out.push(']');
        }
        scalar => write_scalar(scalar, out),
    }
}

/// A value as the `json` that functions such as `json_build_object` build:
/// keys in the order given, `{"k" : v, "k2" : v2}` and `[a, b]`.
pub fn json_text(value: &J) -> String {
    let mut out = String::new();
    write_json(value, &mut out);
    out
}

fn write_json(value: &J, out: &mut String) {
    match value {
        J::Object(map) => {
            out.push('{');
            for (i, (key, item)) in map.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_string(key, out);
                out.push_str(" : ");
                write_json(item, out);
            }
            out.push('}');
        }
        J::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_json(item, out);
            }
            out.push(']');
        }
        scalar => write_scalar(scalar, out),
    }
}

/// A value as `to_json` writes it: a number or boolean bare, text as a
/// JSON string, an array as `[1,2]` and a row as `{"a":1,"b":"x"}` (with
/// `,\n ` between the outer elements when `pretty`), `json` as written,
/// and `jsonb` as its text.
pub(crate) fn value_json(value: &crate::Value, pretty: bool, out: &mut String) {
    use crate::Value;
    let separator = if pretty { ",\n " } else { "," };
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Int(i) => out.push_str(&i.to_string()),
        Value::Numeric(d) => out.push_str(&d.to_string()),
        Value::Float(f) if f.is_finite() => out.push_str(&crate::render(value)),
        // Not a JSON number, so a string of `float8`'s text for it.
        Value::Float(f) => write_string(
            if f.is_nan() {
                "NaN"
            } else if *f > 0.0 {
                "Infinity"
            } else {
                "-Infinity"
            },
            out,
        ),
        Value::Text(s) => write_string(s, out),
        Value::Json(text) => out.push_str(text),
        Value::Jsonb(j) => write_jsonb(j, out),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(separator);
                }
                value_json(item, false, out);
            }
            out.push(']');
        }
        Value::Record(fields) => {
            out.push('{');
            for (i, (name, item)) in fields.iter().enumerate() {
                if i > 0 {
                    out.push_str(separator);
                }
                write_string(name, out);
                out.push(':');
                value_json(item, false, out);
            }
            out.push('}');
        }
    }
}

/// An object key as the JSON builders write one: the value's text as a
/// JSON string. It must be a scalar.
pub(crate) fn key_json(value: &crate::Value, out: &mut String) -> Result<(), String> {
    use crate::Value;
    match value {
        Value::Null => Err("null value not allowed for object key".to_string()),
        Value::Array(_) | Value::Record(_) | Value::Json(_) | Value::Jsonb(_) => {
            Err("key value must be scalar, not array, composite, or json".to_string())
        }
        Value::Bool(b) => {
            write_string(if *b { "true" } else { "false" }, out);
            Ok(())
        }
        other => {
            write_string(&crate::render(other), out);
            Ok(())
        }
    }
}

/// `json_strip_nulls`: the text without its object fields that are null
/// (and, when `in_arrays`, its null array elements), rewritten without
/// whitespace, other values as written.
pub(crate) fn strip_nulls_text(text: &str, in_arrays: bool) -> Option<String> {
    let trimmed = text.trim();
    let is_null = |v: &str| v.trim() == "null";
    let mut out = String::new();
    if let Some(members) = object_members(trimmed) {
        out.push('{');
        for (i, (key, value)) in members.into_iter().filter(|(_, v)| !is_null(v)).enumerate() {
            if i > 0 {
                out.push(',');
            }
            write_string(&key, &mut out);
            out.push(':');
            out.push_str(&strip_nulls_text(value, in_arrays)?);
        }
        out.push('}');
    } else if let Some(elements) = array_elements(trimmed) {
        out.push('[');
        let kept = elements.into_iter().filter(|v| !in_arrays || !is_null(v));
        for (i, element) in kept.enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(&strip_nulls_text(element, in_arrays)?);
        }
        out.push(']');
    } else {
        out.push_str(trimmed);
    }
    Some(out)
}

/// The members of a `json` object's text: each key, decoded, with its value
/// as written. `None` when the text is not an object.
pub(crate) fn object_members(text: &str) -> Option<Vec<(String, &str)>> {
    let b = text.as_bytes();
    let mut i = skip_space(b, 0);
    if b.get(i) != Some(&b'{') {
        return None;
    }
    i = skip_space(b, i + 1);
    let mut members = Vec::new();
    if b.get(i) == Some(&b'}') {
        return Some(members);
    }
    loop {
        let key_end = string_end(b, i)?;
        let key: String = serde_json::from_str(&text[i..key_end]).ok()?;
        i = skip_space(b, key_end);
        if b.get(i) != Some(&b':') {
            return None;
        }
        i = skip_space(b, i + 1);
        let end = value_end(b, i)?;
        members.push((key, &text[i..end]));
        i = skip_space(b, end);
        match b.get(i)? {
            b',' => i = skip_space(b, i + 1),
            b'}' => return Some(members),
            _ => return None,
        }
    }
}

/// The elements of a `json` array's text, as written. `None` when the text
/// is not an array.
pub(crate) fn array_elements(text: &str) -> Option<Vec<&str>> {
    let b = text.as_bytes();
    let mut i = skip_space(b, 0);
    if b.get(i) != Some(&b'[') {
        return None;
    }
    i = skip_space(b, i + 1);
    let mut elements = Vec::new();
    if b.get(i) == Some(&b']') {
        return Some(elements);
    }
    loop {
        let end = value_end(b, i)?;
        elements.push(&text[i..end]);
        i = skip_space(b, end);
        match b.get(i)? {
            b',' => i = skip_space(b, i + 1),
            b']' => return Some(elements),
            _ => return None,
        }
    }
}

/// A member of a `json` value's text, as written: an object's field (the
/// last, when the key repeats) or an array's element (from the end when
/// negative).
pub(crate) fn json_member<'a>(text: &'a str, key: &crate::Value) -> Option<&'a str> {
    match key {
        crate::Value::Int(i) => {
            let elements = array_elements(text)?;
            let index = if *i < 0 {
                elements.len().checked_sub(i.unsigned_abs() as usize)?
            } else {
                *i as usize
            };
            elements.get(index).copied()
        }
        key => {
            let key = crate::render(key);
            object_members(text)?
                .into_iter()
                .rev()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v)
        }
    }
}

/// A `json` member's text as `->>` gives it: a string decoded, `null` as
/// NULL, anything else as written.
pub(crate) fn json_member_text(member: &str) -> crate::Value {
    let trimmed = member.trim();
    if trimmed == "null" {
        return crate::Value::Null;
    }
    match serde_json::from_str::<String>(trimmed) {
        Ok(s) if trimmed.starts_with('"') => crate::Value::Text(s),
        _ => crate::Value::Text(trimmed.to_string()),
    }
}

fn skip_space(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\n' | b'\r') {
        i += 1;
    }
    i
}

/// Where the string starting at `i` ends, past its closing quote.
fn string_end(b: &[u8], i: usize) -> Option<usize> {
    if b.get(i) != Some(&b'"') {
        return None;
    }
    let mut j = i + 1;
    while j < b.len() {
        match b[j] {
            b'\\' => j += 2,
            b'"' => return Some(j + 1),
            _ => j += 1,
        }
    }
    None
}

/// Where the value starting at `i` ends.
fn value_end(b: &[u8], i: usize) -> Option<usize> {
    match *b.get(i)? {
        b'"' => string_end(b, i),
        b'{' | b'[' => {
            let mut depth = 0usize;
            let mut j = i;
            while j < b.len() {
                match b[j] {
                    b'"' => {
                        j = string_end(b, j)?;
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(j + 1);
                        }
                    }
                    _ => {}
                }
                j += 1;
            }
            None
        }
        _ => {
            let mut j = i;
            while j < b.len() && !matches!(b[j], b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r')
            {
                j += 1;
            }
            (j > i).then_some(j)
        }
    }
}

/// `jsonb_pretty`: one element per line, indented four spaces a level.
pub(crate) fn jsonb_pretty(value: &J) -> String {
    let mut out = String::new();
    write_pretty(value, 0, &mut out);
    out
}

fn write_pretty(value: &J, depth: usize, out: &mut String) {
    let indent = |out: &mut String, depth: usize| out.push_str(&"    ".repeat(depth));
    let (open, close, items): (char, char, Vec<(Option<&String>, &J)>) = match value {
        J::Object(map) => (
            '{',
            '}',
            sorted_entries(map)
                .into_iter()
                .map(|(k, v)| (Some(k), v))
                .collect(),
        ),
        J::Array(items) => ('[', ']', items.iter().map(|v| (None, v)).collect()),
        scalar => return write_scalar(scalar, out),
    };
    out.push(open);
    out.push('\n');
    for (i, (key, item)) in items.iter().enumerate() {
        if i > 0 {
            out.push_str(",\n");
        }
        indent(out, depth + 1);
        if let Some(key) = key {
            write_string(key, out);
            out.push_str(": ");
        }
        write_pretty(item, depth + 1, out);
    }
    if !items.is_empty() {
        out.push('\n');
    }
    indent(out, depth);
    out.push(close);
}

fn write_scalar(value: &J, out: &mut String) {
    match value {
        J::Null => out.push_str("null"),
        J::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        J::Number(n) => out.push_str(&number_text(n)),
        J::String(s) => write_string(s, out),
        J::Array(_) | J::Object(_) => write_jsonb(value, out),
    }
}

/// A JSON number as `numeric` prints it: as written but never in exponent
/// notation (`1e3` is `1000`, `1.50` stays `1.50`).
fn number_text(n: &serde_json::Number) -> String {
    let text = n.to_string();
    plain_decimal(&text).unwrap_or(text)
}

/// A JSON number's value, exactly where it fits a decimal.
fn number_value(n: &serde_json::Number) -> Option<rust_decimal::Decimal> {
    let text = n.to_string();
    text.parse::<rust_decimal::Decimal>()
        .ok()
        .or_else(|| rust_decimal::Decimal::from_scientific(&text).ok())
}

/// Rewrites a decimal number in exponent notation (`1.5e-7`) in plain
/// notation (`0.00000015`), with a zero unsigned as `numeric` has it.
fn plain_decimal(text: &str) -> Option<String> {
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i64>().ok()?),
        None => (unsigned, 0),
    };
    if exponent.unsigned_abs() > 1000 {
        return None;
    }
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{int_part}{frac_part}");
    let point = int_part.len() as i64 + exponent;
    let scale = (frac_part.len() as i64 - exponent).max(0) as usize;
    let (int_digits, frac_digits) = if point <= 0 {
        (
            "0".to_string(),
            format!("{}{digits}", "0".repeat(point.unsigned_abs() as usize)),
        )
    } else if point as usize >= digits.len() {
        (
            format!("{digits}{}", "0".repeat(point as usize - digits.len())),
            String::new(),
        )
    } else {
        let (i, f) = digits.split_at(point as usize);
        (i.to_string(), f.to_string())
    };
    let int_digits = match int_digits.trim_start_matches('0') {
        "" => "0",
        trimmed => trimmed,
    };
    let frac_digits = format!("{frac_digits:0<scale$}");
    let frac_digits = &frac_digits[..scale.min(frac_digits.len())];
    let zero = int_digits == "0" && frac_digits.bytes().all(|b| b == b'0');
    let sign = if negative && !zero { "-" } else { "" };
    Some(if frac_digits.is_empty() {
        format!("{sign}{int_digits}")
    } else {
        format!("{sign}{int_digits}.{frac_digits}")
    })
}

/// A JSON string literal, escaped as PostgreSQL escapes it.
fn write_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// `jsonb` ordering: objects above arrays above booleans above numbers
/// above strings above null; containers with more members sort higher,
/// then compare member by member (object keys in `jsonb` key order);
/// numbers compare by value.
pub(crate) fn jsonb_cmp(a: &J, b: &J) -> Ordering {
    fn rank(v: &J) -> u8 {
        match v {
            J::Null => 0,
            J::String(_) => 1,
            J::Number(_) => 2,
            J::Bool(_) => 3,
            J::Array(_) => 4,
            J::Object(_) => 5,
        }
    }
    match (a, b) {
        (J::Number(x), J::Number(y)) => match (number_value(x), number_value(y)) {
            (Some(x), Some(y)) => x.cmp(&y),
            _ => {
                let (x, y) = (x.as_f64().unwrap_or(0.0), y.as_f64().unwrap_or(0.0));
                x.partial_cmp(&y).unwrap_or(Ordering::Equal)
            }
        },
        (J::String(x), J::String(y)) => x.as_bytes().cmp(y.as_bytes()),
        (J::Bool(x), J::Bool(y)) => x.cmp(y),
        (J::Array(x), J::Array(y)) => x.len().cmp(&y.len()).then_with(|| {
            x.iter()
                .zip(y)
                .map(|(p, q)| jsonb_cmp(p, q))
                .find(|o| o.is_ne())
                .unwrap_or(Ordering::Equal)
        }),
        (J::Object(x), J::Object(y)) => x.len().cmp(&y.len()).then_with(|| {
            sorted_entries(x)
                .into_iter()
                .zip(sorted_entries(y))
                .map(|((kx, vx), (ky, vy))| key_order(kx, ky).then_with(|| jsonb_cmp(vx, vy)))
                .find(|o| o.is_ne())
                .unwrap_or(Ordering::Equal)
        }),
        _ => rank(a).cmp(&rank(b)),
    }
}

/// `jsonb || jsonb`: objects merge (the right side's keys win), arrays
/// concatenate, and a scalar or object joins an array as one element.
pub(crate) fn jsonb_concat(left: J, right: J) -> J {
    match (left, right) {
        (J::Object(mut l), J::Object(r)) => {
            for (key, value) in r {
                l.insert(key, value);
            }
            J::Object(l)
        }
        (J::Array(mut l), J::Array(r)) => {
            l.extend(r);
            J::Array(l)
        }
        (J::Array(mut l), r) => {
            l.push(r);
            J::Array(l)
        }
        (l, J::Array(r)) => J::Array(std::iter::once(l).chain(r).collect()),
        (l, r) => J::Array(vec![l, r]),
    }
}

/// `jsonb - key` / `jsonb - index`: an object without the key, an array
/// without its string elements equal to the key, or without the element at
/// the (possibly negative) index.
pub(crate) fn jsonb_delete(value: J, key: &crate::Value) -> Result<J, String> {
    match (value, key) {
        (J::Object(mut map), crate::Value::Text(k)) => {
            map.shift_remove(k.as_str());
            Ok(J::Object(map))
        }
        (J::Array(items), crate::Value::Text(k)) => Ok(J::Array(
            items
                .into_iter()
                .filter(|item| item.as_str() != Some(k.as_str()))
                .collect(),
        )),
        (J::Array(mut items), crate::Value::Int(i)) => {
            let index = if *i < 0 { items.len() as i64 + i } else { *i };
            if let Ok(index) = usize::try_from(index)
                && index < items.len()
            {
                items.remove(index);
            }
            Ok(J::Array(items))
        }
        (value @ J::Object(_), crate::Value::Array(keys)) => keys
            .iter()
            .try_fold(value, |acc, key| jsonb_delete(acc, key)),
        (J::Object(_), crate::Value::Int(_)) => {
            Err("cannot delete from object using integer index".to_string())
        }
        _ => Err("cannot delete from scalar".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn jsonb_text_orders_keys_shorter_first_and_spaces_separators() {
        let value =
            parse(r#"{"b": 1, "aa": [1, {"z": null, "y": true}], "a": "x", "b": 2}"#).unwrap();
        assert_eq!(
            jsonb_text(&value),
            r#"{"a": "x", "b": 2, "aa": [1, {"y": true, "z": null}]}"#
        );
    }

    #[test]
    fn numbers_print_without_exponents() {
        assert_eq!(plain_decimal("1e20").unwrap(), "100000000000000000000");
        assert_eq!(plain_decimal("1.5e-7").unwrap(), "0.00000015");
        assert_eq!(plain_decimal("-0.0").unwrap(), "0.0");
        assert_eq!(plain_decimal("2.50").unwrap(), "2.50");
        assert_eq!(
            jsonb_text(&json!([1e20, 0.5, -3])),
            "[100000000000000000000, 0.5, -3]"
        );
    }

    #[test]
    fn json_members_are_sliced_as_written() {
        let text = r#" {"a" :  [1, {"x":"}"}], "b": "q\"", "a": 2 } "#;
        let members = object_members(text).unwrap();
        let keys: Vec<&str> = members.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["a", "b", "a"]);
        assert_eq!(members[0].1, r#"[1, {"x":"}"}]"#);
        // A repeated key reads its last value; negative indexes count back.
        assert_eq!(
            json_member(text, &crate::Value::Text("a".into())),
            Some("2")
        );
        assert_eq!(
            array_elements("[1, [2,3] ,\"x\"]").unwrap(),
            ["1", "[2,3]", "\"x\""]
        );
        assert_eq!(json_member("[1, 2]", &crate::Value::Int(-1)), Some("2"));
        assert_eq!(json_member("[1, 2]", &crate::Value::Int(-3)), None);
        assert_eq!(object_members("[1]"), None);
        assert_eq!(
            json_member_text(r#""a\nb""#),
            crate::Value::Text("a\nb".into())
        );
        assert_eq!(json_member_text("null"), crate::Value::Null);
        assert_eq!(json_member_text("{ }"), crate::Value::Text("{ }".into()));
    }

    #[test]
    fn values_write_as_to_json_does() {
        use crate::Value;
        let row = Value::Record(vec![
            ("a".into(), Value::Int(1)),
            (
                "b".into(),
                Value::Array(vec![Value::Text("x".into()), Value::Null]),
            ),
            ("c".into(), Value::Json(r#"{"k" :  1}"#.into())),
            ("d".into(), Value::Float(f64::NAN)),
        ]);
        let mut out = String::new();
        value_json(&row, false, &mut out);
        assert_eq!(out, r#"{"a":1,"b":["x",null],"c":{"k" :  1},"d":"NaN"}"#);
        let mut out = String::new();
        value_json(
            &Value::Array(vec![Value::Int(1), Value::Int(2)]),
            true,
            &mut out,
        );
        assert_eq!(out, "[1,\n 2]");
        let mut key = String::new();
        assert!(key_json(&Value::Bool(true), &mut key).is_ok());
        assert_eq!(key, r#""true""#);
        assert!(key_json(&Value::Array(vec![]), &mut String::new()).is_err());
    }

    #[test]
    fn strip_nulls_keeps_numbers_as_written() {
        assert_eq!(
            strip_nulls_text(r#"{"a": null, "b": [1e3, null, {"c": null}]}"#, false).unwrap(),
            r#"{"b":[1e3,null,{}]}"#
        );
        assert_eq!(strip_nulls_text("[1, null]", true).unwrap(), "[1]");
    }

    #[test]
    fn json_text_keeps_key_order() {
        let value = json!({"k": 1, "b": ["x", null]});
        assert_eq!(json_text(&value), r#"{"k" : 1, "b" : ["x", null]}"#);
    }

    #[test]
    fn pretty_matches_postgresql_layout() {
        let value = json!({"a": [1, {"b": 2}], "c": {}});
        assert_eq!(
            jsonb_pretty(&value),
            "{\n    \"a\": [\n        1,\n        {\n            \"b\": 2\n        }\n    ],\n    \"c\": {\n    }\n}"
        );
    }

    #[test]
    fn strings_escape_control_characters() {
        assert_eq!(jsonb_text(&json!("a\"\\\t\u{1}é")), r#""a\"\\\t\u0001é""#);
    }

    #[test]
    fn ordering_and_operators_follow_jsonb() {
        assert_eq!(
            jsonb_cmp(&json!({"a": 1.0}), &json!({"a": 1})),
            Ordering::Equal
        );
        assert_eq!(jsonb_cmp(&json!([1]), &json!({})), Ordering::Less);
        assert_eq!(
            jsonb_concat(json!({"a": 1, "b": 2}), json!({"b": 3})),
            json!({"a": 1, "b": 3})
        );
        assert_eq!(jsonb_concat(json!([1]), json!(2)), json!([1, 2]));
        assert_eq!(
            jsonb_delete(json!([1, 2, 3]), &crate::Value::Int(-1)).unwrap(),
            json!([1, 2])
        );
        assert_eq!(
            jsonb_delete(json!({"a": 1, "b": 2}), &crate::Value::Text("a".into())).unwrap(),
            json!({"b": 2})
        );
    }
}
