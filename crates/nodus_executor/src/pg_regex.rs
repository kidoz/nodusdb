//! PostgreSQL regular expressions on the `regex` crate: the flags the
//! `regexp_*` functions take, the escapes PostgreSQL's dialect spells
//! differently (`\y`, `\m`, `[[:<:]]`), and the functions themselves, with
//! their start positions, occurrence numbers, and subexpressions.

use std::cell::RefCell;
use std::collections::HashMap;

/// Compiles a pattern with `regexp_*` flags (`i`, `c`, `n`, `m`, `p`, `w`,
/// `s`, `x`, `q`; `g` is the caller's). Patterns are usually constant per
/// statement, so compiled ones are cached per thread.
pub(crate) fn compile(pattern: &str, flags: &str) -> Result<regex::Regex, String> {
    thread_local! {
        static CACHE: RefCell<HashMap<(String, String), Result<regex::Regex, String>>> =
            RefCell::new(HashMap::new());
    }
    let key = (pattern.to_string(), flags.to_string());
    if let Some(hit) = CACHE.with(|c| c.borrow().get(&key).cloned()) {
        return hit;
    }
    let compiled = build(pattern, flags);
    CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        if cache.len() >= 256 {
            cache.clear();
        }
        cache.insert(key, compiled.clone());
    });
    compiled
}

fn build(pattern: &str, flags: &str) -> Result<regex::Regex, String> {
    let (mut insensitive, mut multi_line, mut dot_all) = (false, false, true);
    let (mut extended, mut literal) = (false, false);
    for flag in flags.chars() {
        match flag {
            'i' => insensitive = true,
            'c' => insensitive = false,
            'n' | 'm' => (multi_line, dot_all) = (true, false),
            'p' => (multi_line, dot_all) = (false, false),
            'w' => (multi_line, dot_all) = (true, true),
            's' => (multi_line, dot_all) = (false, true),
            'x' => extended = true,
            'q' => literal = true,
            'g' => {}
            other => return Err(format!("invalid regular expression option: \"{other}\"")),
        }
    }
    let source = if literal {
        regex::escape(pattern)
    } else {
        translate(pattern)
    };
    regex::RegexBuilder::new(&source)
        .case_insensitive(insensitive)
        .multi_line(multi_line)
        .dot_matches_new_line(dot_all)
        .ignore_whitespace(extended)
        .build()
        .map_err(|_| format!("invalid regular expression: {pattern}"))
}

/// PostgreSQL's regex escapes in the `regex` crate's spelling.
fn translate(pattern: &str) -> String {
    let pattern = pattern.replace("[[:<:]]", "\\<").replace("[[:>:]]", "\\>");
    let mut out = String::with_capacity(pattern.len());
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('y') => out.push_str("\\b"),
            Some('Y') => out.push_str("\\B"),
            Some('m') => out.push_str("\\<"),
            Some('M') => out.push_str("\\>"),
            Some('Z') => out.push_str("\\z"),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

/// The byte offset of a 1-based character position (past the end when the
/// text is shorter).
fn byte_offset(text: &str, position: i64) -> usize {
    text.char_indices()
        .nth((position - 1).max(0) as usize)
        .map_or(text.len(), |(i, _)| i)
}

/// The 1-based character position of a byte offset.
fn char_position(text: &str, offset: usize) -> i64 {
    text[..offset].chars().count() as i64 + 1
}

fn check_parameter(name: &str, value: i64, min: i64) -> Result<(), String> {
    if value < min {
        Err(format!("invalid value for parameter \"{name}\": {value}"))
    } else {
        Ok(())
    }
}

/// The matches at or after a start position, each as its captures.
fn matches_from<'t>(
    re: &regex::Regex,
    text: &'t str,
    start: i64,
) -> Result<Vec<regex::Captures<'t>>, String> {
    check_parameter("start", start, 1)?;
    let from = byte_offset(text, start);
    let mut out = Vec::new();
    let mut at = from;
    while at <= text.len() {
        let Some(caps) = re.captures_at(text, at) else {
            break;
        };
        let whole = caps.get(0).map_or(at..at, |m| m.range());
        at = if whole.is_empty() {
            // Step past an empty match by one character.
            whole.end + text[whole.end..].chars().next().map_or(1, char::len_utf8)
        } else {
            whole.end
        };
        out.push(caps);
    }
    Ok(out)
}

/// `regexp_count(string, pattern [, start [, flags]])`.
pub(crate) fn count(text: &str, pattern: &str, start: i64, flags: &str) -> Result<i64, String> {
    let re = compile(pattern, flags)?;
    Ok(matches_from(&re, text, start)?.len() as i64)
}

/// `regexp_instr(string, pattern [, start [, n [, endoption [, flags [,
/// subexpr]]]]])`: where the `n`th match (or its subexpression) starts, or
/// ends with `endoption` 1; 0 without one.
pub(crate) fn instr(
    text: &str,
    pattern: &str,
    start: i64,
    n: i64,
    end_option: i64,
    flags: &str,
    subexpr: i64,
) -> Result<i64, String> {
    check_parameter("n", n, 1)?;
    if !(0..=1).contains(&end_option) {
        return Err(format!(
            "invalid value for parameter \"endoption\": {end_option}"
        ));
    }
    check_parameter("subexpr", subexpr, 0)?;
    let re = compile(pattern, flags)?;
    let found = matches_from(&re, text, start)?;
    let Some(m) = found
        .get((n - 1) as usize)
        .and_then(|caps| caps.get(subexpr as usize))
    else {
        return Ok(0);
    };
    Ok(char_position(
        text,
        if end_option == 1 { m.end() } else { m.start() },
    ))
}

/// `regexp_substr(string, pattern [, start [, n [, flags [, subexpr]]]])`.
pub(crate) fn substr(
    text: &str,
    pattern: &str,
    start: i64,
    n: i64,
    flags: &str,
    subexpr: i64,
) -> Result<Option<String>, String> {
    check_parameter("n", n, 1)?;
    check_parameter("subexpr", subexpr, 0)?;
    let re = compile(pattern, flags)?;
    let found = matches_from(&re, text, start)?;
    Ok(found
        .get((n - 1) as usize)
        .and_then(|caps| caps.get(subexpr as usize))
        .map(|m| m.as_str().to_string()))
}

/// `regexp_replace(string, pattern, replacement [, start [, n]] [, flags])`:
/// the `n`th match at or after `start` replaced, or every one with `n` 0 or
/// the `g` flag.
pub(crate) fn replace(
    text: &str,
    pattern: &str,
    replacement: &str,
    start: i64,
    n: Option<i64>,
    flags: &str,
) -> Result<String, String> {
    if let Some(n) = n {
        check_parameter("n", n, 0)?;
    }
    let re = compile(pattern, flags)?;
    let all = flags.contains('g') || n == Some(0);
    let wanted = n.unwrap_or(1).max(1) as usize;
    let found = matches_from(&re, text, start)?;
    let mut out = String::with_capacity(text.len());
    let mut copied = 0;
    for (i, caps) in found.iter().enumerate() {
        if !all && i + 1 != wanted {
            continue;
        }
        let whole = caps.get(0).map_or(0..0, |m| m.range());
        out.push_str(&text[copied..whole.start]);
        expand(caps, replacement, &mut out);
        copied = whole.end;
        if !all {
            break;
        }
    }
    out.push_str(&text[copied..]);
    Ok(out)
}

/// A replacement with `\1`..`\9` and `\&` (the whole match) filled in; `\\`
/// is a backslash.
fn expand(caps: &regex::Captures<'_>, replacement: &str, out: &mut String) {
    let mut chars = replacement.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some(d @ '1'..='9') => {
                if let Some(m) = caps.get(d as usize - '0' as usize) {
                    out.push_str(m.as_str());
                }
            }
            Some('&') => out.push_str(caps.get(0).map_or("", |m| m.as_str())),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
}

/// A match as `regexp_match` shows it: its parenthesized subexpressions
/// (NULL where one did not take part), or the whole match without any.
pub(crate) fn match_array(caps: &regex::Captures<'_>) -> crate::Value {
    use crate::Value;
    if caps.len() == 1 {
        return Value::Array(vec![Value::Text(caps[0].to_string())]);
    }
    Value::Array(
        caps.iter()
            .skip(1)
            .map(|m| m.map_or(Value::Null, |m| Value::Text(m.as_str().to_string())))
            .collect(),
    )
}

/// `regexp_matches(string, pattern [, flags])`: the first match, or with the
/// `g` flag every one, each as [`match_array`] shows it.
pub(crate) fn matches(text: &str, pattern: &str, flags: &str) -> Result<Vec<crate::Value>, String> {
    let re = compile(pattern, flags)?;
    let found = matches_from(&re, text, 1)?;
    let take = if flags.contains('g') { found.len() } else { 1 };
    Ok(found.iter().take(take).map(match_array).collect())
}

/// `regexp_split_to_array` / `regexp_split_to_table`: the text between the
/// matches. An empty match splits between characters, but none splits at
/// the ends.
pub(crate) fn split(text: &str, pattern: &str, flags: &str) -> Result<Vec<String>, String> {
    if flags.contains('g') {
        return Err("regexp_split_to_table() does not support the \"global\" option".into());
    }
    let re = compile(pattern, flags)?;
    let mut pieces = Vec::new();
    let mut copied = 0;
    for caps in matches_from(&re, text, 1)? {
        let whole = caps.get(0).map_or(0..0, |m| m.range());
        if whole.is_empty() && (whole.start == 0 || whole.start >= text.len()) {
            continue;
        }
        pieces.push(text[copied..whole.start].to_string());
        copied = whole.end;
    }
    pieces.push(text[copied..].to_string());
    Ok(pieces)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn functions_take_postgresql_arguments() {
        assert_eq!(count("banana", "a", 3, ""), Ok(2));
        assert_eq!(count("ABA", "a", 1, "i"), Ok(2));
        assert_eq!(instr("banana", "an", 1, 2, 0, "", 0), Ok(4));
        assert_eq!(instr("banana", "an", 1, 1, 1, "", 0), Ok(4));
        assert_eq!(instr("abc", "x", 1, 1, 0, "", 0), Ok(0));
        assert_eq!(instr("a1b2", "([a-z])(\\d)", 1, 2, 0, "", 2), Ok(4));
        assert_eq!(substr("banana", "a.a", 1, 2, "", 0), Ok(None));
        assert_eq!(
            substr("a1b2", "([a-z])(\\d)", 1, 2, "", 2),
            Ok(Some("2".into()))
        );
        assert_eq!(
            replace("banana", "a", "X", 1, Some(2), "").unwrap(),
            "banXna"
        );
        assert_eq!(replace("banana", "a", "X", 3, None, "").unwrap(), "banXna");
        assert_eq!(
            replace("banana", "a", "X", 1, Some(0), "g").unwrap(),
            "bXnXnX"
        );
        assert_eq!(
            replace("Hello", "l+", "<\\&>", 1, None, "").unwrap(),
            "He<ll>o"
        );
        assert_eq!(split("one  two", "\\s+", "").unwrap(), vec!["one", "two"]);
        assert_eq!(split("abc", "", "").unwrap(), vec!["a", "b", "c"]);
        assert!(compile("a", "z").is_err());
        // `.` matches a newline unless the pattern is newline-sensitive.
        assert!(compile("a.b", "").unwrap().is_match("a\nb"));
        assert!(!compile("a.b", "n").unwrap().is_match("a\nb"));
        assert!(compile("\\ybar\\y", "").unwrap().is_match("foo bar"));
    }
}
