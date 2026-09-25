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

/// A JSON number as `numeric` prints it: never in exponent notation.
fn number_text(n: &serde_json::Number) -> String {
    let text = n.to_string();
    if n.is_f64() {
        plain_decimal(&text).unwrap_or(text)
    } else {
        text
    }
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
        (J::Number(x), J::Number(y)) => {
            let (x, y) = (x.as_f64().unwrap_or(0.0), y.as_f64().unwrap_or(0.0));
            x.partial_cmp(&y).unwrap_or(Ordering::Equal)
        }
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
