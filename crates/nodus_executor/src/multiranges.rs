//! Multiranges: sets of ranges of one subtype, kept sorted, non-overlapping,
//! and non-adjacent, as canonical text (`{[1,2),[3,4)}`, `{}` when empty).

use crate::Value;
use crate::ranges::{self, Kind, Range};

/// Whether a declared type names a multirange type.
pub(crate) fn is_multirange_type(data_type: &str) -> bool {
    Kind::of_multirange(data_type).is_some()
}

/// The range subtype a declared multirange type names.
pub(crate) fn kind_of(data_type: &str) -> Option<Kind> {
    Kind::of_multirange(data_type)
}

/// Whether a value's text is a multirange (`{...}`); a range's never is.
pub(crate) fn is_multirange_text(text: &str) -> bool {
    text.trim_start().starts_with('{')
}

fn malformed(text: &str, detail: &str) -> String {
    crate::error_fields::DbError::new(format!("malformed multirange literal: \"{text}\""))
        .code("22P02")
        .detail(detail)
        .into_text()
}

/// The multirange's elements, from canonical text: `{}` has none.
pub(crate) fn parse(text: &str) -> Result<Vec<Range>, String> {
    let trimmed = text.trim();
    let chars: Vec<char> = trimmed.chars().collect();
    let Some('{') = chars.first() else {
        return Err(malformed(trimmed, "Missing left brace."));
    };
    let Some(close) = matching_brace(&chars) else {
        return Err(malformed(trimmed, "Unexpected end of input."));
    };
    let inner: String = chars[1..close].iter().collect();
    split_elements(trimmed, &inner)?
        .iter()
        .map(|element| ranges::parse(element))
        .collect()
}

/// A literal as the type takes it: parsed, typed, merged, and canonical.
pub(crate) fn from_literal(kind: Kind, text: &str) -> Result<String, String> {
    let trimmed = text.trim();
    let chars: Vec<char> = trimmed.chars().collect();
    let Some('{') = chars.first() else {
        return Err(malformed(trimmed, "Missing left brace."));
    };
    let Some(close) = matching_brace(&chars) else {
        return Err(malformed(trimmed, "Unexpected end of input."));
    };
    if close != chars.len() - 1 {
        return Err(malformed(trimmed, "Junk after closing right brace."));
    }
    let inner: String = chars[1..close].iter().collect();
    let mut elements: Vec<Range> = Vec::new();
    for element in split_elements(trimmed, &inner)? {
        // Each element is typed and canonicalized as a range of the subtype.
        let canonical = ranges::from_literal(kind, &element)?;
        let range = ranges::parse(&canonical)?;
        if range != Range::Empty {
            elements.push(range);
        }
    }
    Ok(format(&merge(kind, elements)))
}

/// The index of the `}` that closes the opening `{`, ignoring braces inside
/// quoted bounds.
fn matching_brace(chars: &[char]) -> Option<usize> {
    let mut quoted = false;
    let mut escaped = false;
    for (i, c) in chars.iter().enumerate().skip(1) {
        if escaped {
            escaped = false;
        } else if quoted && *c == '\\' {
            escaped = true;
        } else if *c == '"' {
            quoted = !quoted;
        } else if *c == '}' && !quoted {
            return Some(i);
        }
    }
    None
}

/// The elements of the braces' inside, split at the commas between ranges.
fn split_elements(multirange: &str, inner: &str) -> Result<Vec<String>, String> {
    let chars: Vec<char> = inner.chars().collect();
    let mut elements = Vec::new();
    let mut i = 0;
    let mut after_comma = false;
    loop {
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        if i == chars.len() {
            if after_comma {
                return Err(malformed(multirange, "Expected range start."));
            }
            return Ok(elements);
        }
        after_comma = false;
        // The word `empty` is an element too (it merges away).
        let rest: String = chars[i..].iter().take(5).collect();
        if rest.eq_ignore_ascii_case("empty") {
            let after = chars.get(i + 5);
            if after.is_none_or(|c| c.is_whitespace() || *c == ',') {
                elements.push("empty".to_string());
                i += 5;
                while i < chars.len() && chars[i].is_whitespace() {
                    i += 1;
                }
                match chars.get(i) {
                    None => return Ok(elements),
                    Some(',') => {
                        i += 1;
                        after_comma = true;
                        continue;
                    }
                    Some(_) => return Err(malformed(multirange, "Expected range delimiter.")),
                }
            }
        }
        if !matches!(chars[i], '[' | '(') {
            return Err(malformed(multirange, "Expected range start."));
        }
        let start = i;
        // The range's closing bracket, ignoring quoted brackets.
        let mut quoted = false;
        let mut escaped = false;
        while i < chars.len() {
            let c = chars[i];
            if escaped {
                escaped = false;
            } else if quoted && c == '\\' {
                escaped = true;
            } else if c == '"' {
                quoted = !quoted;
            } else if !quoted && matches!(c, ']' | ')') {
                break;
            }
            i += 1;
        }
        if i == chars.len() {
            return Err(malformed(multirange, "Unexpected end of input."));
        }
        elements.push(chars[start..=i].iter().collect());
        i += 1;
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        match chars.get(i) {
            None => return Ok(elements),
            Some(',') => {
                i += 1;
                after_comma = true;
            }
            Some(_) => return Err(malformed(multirange, "Expected range delimiter.")),
        }
    }
}

/// A multirange as PostgreSQL writes it: the elements' texts in braces.
pub(crate) fn format(elements: &[Range]) -> String {
    let texts: Vec<String> = elements.iter().map(ranges::format_range).collect();
    format!("{{{}}}", texts.join(","))
}

/// The elements sorted and merged: overlapping and adjacent ranges join, as
/// PostgreSQL's multirange canonicalization does.
pub(crate) fn merge(kind: Kind, mut elements: Vec<Range>) -> Vec<Range> {
    elements.sort_by(|a, b| ranges::cmp_ranges(kind, a, b));
    let mut merged: Vec<Range> = Vec::new();
    for range in elements {
        match merged.last() {
            Some(last) if touching(kind, last, &range) => {
                let joined = ranges::union(kind, last, &range)
                    .expect("overlapping or adjacent ranges have a contiguous union");
                *merged.last_mut().expect("checked above") = joined;
            }
            _ => merged.push(range),
        }
    }
    merged
}

/// Whether two sorted ranges overlap or touch (so a multirange holds them as
/// one).
fn touching(kind: Kind, a: &Range, b: &Range) -> bool {
    ranges::overlaps(kind, a, b) || ranges::adjacent(kind, a, b) || ranges::adjacent(kind, b, a)
}

/// The comparison of two multiranges: element by element, a prefix first.
pub(crate) fn cmp(kind: Kind, a: &[Range], b: &[Range]) -> std::cmp::Ordering {
    for (x, y) in a.iter().zip(b) {
        let ord = ranges::cmp_ranges(kind, x, y);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    a.len().cmp(&b.len())
}

/// A bound of the first (`lower`) or last element, as the range functions
/// read one.
pub(crate) fn bound_value(text: &str, lower: bool) -> Result<Value, String> {
    let elements = parse(text)?;
    let element = if lower {
        elements.first()
    } else {
        elements.last()
    };
    match element {
        None => Ok(Value::Null),
        Some(range) => ranges::bound_value(&ranges::format_range(range), lower),
    }
}

pub(crate) fn bound_inc(text: &str, lower: bool) -> Result<Value, String> {
    let elements = parse(text)?;
    let element = if lower {
        elements.first()
    } else {
        elements.last()
    };
    match element {
        None => Ok(Value::Bool(false)),
        Some(range) => ranges::bound_inc(&ranges::format_range(range), lower),
    }
}

pub(crate) fn bound_inf(text: &str, lower: bool) -> Result<Value, String> {
    let elements = parse(text)?;
    let element = if lower {
        elements.first()
    } else {
        elements.last()
    };
    match element {
        None => Ok(Value::Bool(false)),
        Some(range) => ranges::bound_inf(&ranges::format_range(range), lower),
    }
}

pub(crate) fn is_empty(text: &str) -> Result<Value, String> {
    Ok(Value::Bool(parse(text)?.is_empty()))
}

/// `range_merge(multirange)`: one range from the first lower to the last
/// upper, `empty` when there is none.
pub(crate) fn range_merge(text: &str) -> Result<Value, String> {
    let elements = parse(text)?;
    Ok(Value::Text(span(&elements)))
}

/// The range from the first element's lower bound to the last one's upper.
fn span(elements: &[Range]) -> String {
    let (Some(first), Some(last)) = (elements.first(), elements.last()) else {
        return "empty".to_string();
    };
    match (first, last) {
        (Range::Bounds { lower, .. }, Range::Bounds { upper, .. }) => {
            ranges::format_range(&Range::Bounds {
                lower: lower.clone(),
                upper: upper.clone(),
            })
        }
        _ => "empty".to_string(),
    }
}

/// `unnest(multirange)`: each element, as a range of the subtype.
pub(crate) fn unnest(text: &str) -> Result<Vec<Value>, String> {
    Ok(parse(text)?
        .iter()
        .map(|range| Value::Text(ranges::format_range(range)))
        .collect())
}

/// `range_agg` over ranges or multiranges: all the elements merged into one
/// multirange.
pub(crate) fn merge_ranges(kind: Kind, texts: &[String]) -> String {
    let mut all = Vec::new();
    for text in texts {
        if let Ok(elements) = side(kind, text) {
            all.extend(elements.into_iter().filter(|r| *r != Range::Empty));
        }
    }
    format(&merge(kind, all))
}

/// `range_intersect_agg`: the ranges common to every value. Ranges keep a
/// range result; multiranges a multirange.
pub(crate) fn intersect_all(kind: Kind, texts: &[String], multirange: bool) -> Value {
    let mut common: Option<Vec<Range>> = None;
    for text in texts {
        let Ok(elements) = side(kind, text) else {
            return Value::Null;
        };
        let next = match &common {
            None => elements,
            Some(current) => {
                let mut pieces = Vec::new();
                for a in current {
                    for b in &elements {
                        let piece = ranges::intersection(kind, a, b);
                        if piece != Range::Empty {
                            pieces.push(piece);
                        }
                    }
                }
                merge(kind, pieces)
            }
        };
        common = Some(next);
    }
    let Some(elements) = common else {
        return Value::Null;
    };
    if multirange {
        return Value::Text(format(&elements));
    }
    Value::Text(span(&elements))
}

/// The bounds of a `tstzmultirange`'s elements move to the session's zone.
pub(crate) fn localize(text: &str) -> String {
    let Ok(elements) = parse(text) else {
        return text.to_string();
    };
    let localized: Vec<String> = elements
        .iter()
        .map(|range| ranges::localize(&ranges::format_range(range)))
        .collect();
    format!("{{{}}}", localized.join(","))
}

/// A multirange operator's result. `right_is_element` marks `@>` on an
/// element; `<@` arrives with its operands swapped.
pub(crate) fn operator(
    op: &str,
    kind: Kind,
    left: &str,
    right: &str,
    right_is_element: bool,
) -> Result<Value, String> {
    // Each side reads as its elements: a multirange's, or a range's single.
    let left_side = side(kind, left)?;
    let right_side = if right_is_element {
        Vec::new()
    } else {
        side(kind, right)?
    };
    let left_is_multirange = is_multirange_text(left);
    let right_is_multirange = !right_is_element && is_multirange_text(right);
    let holds = match op {
        "@>" => {
            if right_is_element {
                let element = ranges::typed(kind, right)?;
                left_side
                    .iter()
                    .any(|range| ranges::contains_element(kind, range, &element))
            } else {
                // Every range of the right side lies within one of the left.
                right_side.iter().all(|rb| {
                    left_side
                        .iter()
                        .any(|ra| ranges::contains_range(kind, ra, rb))
                })
            }
        }
        "&&" => left_side
            .iter()
            .any(|ra| right_side.iter().any(|rb| ranges::overlaps(kind, ra, rb))),
        "<<" | ">>" | "&<" | "&>" => {
            if left_side.is_empty() || right_side.is_empty() {
                return Ok(Value::Bool(false));
            }
            let left_first = left_side.first().expect("checked non-empty");
            let left_last = left_side.last().expect("checked non-empty");
            let right_first = right_side.first().expect("checked non-empty");
            let right_last = right_side.last().expect("checked non-empty");
            match op {
                "<<" => ranges::strictly_left(kind, left_last, right_first),
                ">>" => ranges::strictly_left(kind, right_last, left_first),
                "&<" => ranges::not_right_of(kind, left_last, right_last),
                _ => ranges::not_left_of(kind, left_first, right_first),
            }
        }
        "-|-" => {
            if left_side.is_empty() || right_side.is_empty() {
                return Ok(Value::Bool(false));
            }
            ranges::adjacent(
                kind,
                left_side.last().expect("checked non-empty"),
                right_side.first().expect("checked non-empty"),
            )
        }
        "+" => {
            let mut all = left_side;
            all.extend(right_side);
            return Ok(Value::Text(format(&merge(kind, all))));
        }
        "*" => {
            let mut pieces = Vec::new();
            for ra in &left_side {
                for rb in &right_side {
                    let piece = ranges::intersection(kind, ra, rb);
                    if piece != Range::Empty {
                        pieces.push(piece);
                    }
                }
            }
            return Ok(Value::Text(format(&merge(kind, pieces))));
        }
        "-" => {
            let mut pieces = left_side;
            for rb in &right_side {
                pieces = pieces
                    .iter()
                    .flat_map(|ra| subtract(kind, ra, rb))
                    .collect();
            }
            return Ok(Value::Text(format(&merge(kind, pieces))));
        }
        "=" | "<>" | "<" | ">" | "<=" | ">=" => {
            if !left_is_multirange || !right_is_multirange {
                return Err(format!("unsupported multirange operator {op}"));
            }
            let ord = cmp(kind, &left_side, &right_side);
            use std::cmp::Ordering::*;
            match op {
                "=" => ord == Equal,
                "<>" => ord != Equal,
                "<" => ord == Less,
                ">" => ord == Greater,
                "<=" => ord != Greater,
                _ => ord != Less,
            }
        }
        other => return Err(format!("unsupported multirange operator {other}")),
    };
    Ok(Value::Bool(holds))
}

/// A value's elements: a multirange's, or a range's single element.
fn side(kind: Kind, text: &str) -> Result<Vec<Range>, String> {
    let _ = kind;
    if is_multirange_text(text) {
        parse(text)
    } else {
        Ok(vec![ranges::parse(text)?])
    }
}

/// One range minus another: no piece where they do not overlap, one where
/// the other covers a side, two where it cuts through.
fn subtract(kind: Kind, a: &Range, b: &Range) -> Vec<Range> {
    if !ranges::overlaps(kind, a, b) {
        return vec![a.clone()];
    }
    let (
        Range::Bounds {
            lower: al,
            upper: au,
        },
        Range::Bounds {
            lower: bl,
            upper: bu,
        },
    ) = (a, b)
    else {
        return Vec::new();
    };
    let mut pieces = Vec::new();
    let left = Range::Bounds {
        lower: al.clone(),
        upper: ranges::Bound {
            value: bl.value.clone(),
            // The other range's lower bound is excluded from the piece when
            // it is included in the removed range.
            inclusive: !bl.inclusive,
        },
    };
    let right = Range::Bounds {
        lower: ranges::Bound {
            value: bu.value.clone(),
            inclusive: !bu.inclusive,
        },
        upper: au.clone(),
    };
    for piece in [left, right] {
        let Range::Bounds { lower, upper } = &piece else {
            continue;
        };
        if let (Some(l), Some(u)) = (&lower.value, &upper.value) {
            match ranges::cmp_value(kind, l, u) {
                std::cmp::Ordering::Greater => continue,
                std::cmp::Ordering::Equal if !(lower.inclusive && upper.inclusive) => continue,
                _ => {}
            }
        }
        pieces.push(piece);
    }
    pieces
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit(kind: Kind, text: &str) -> String {
        from_literal(kind, text).unwrap()
    }

    fn elements(text: &str) -> Vec<Range> {
        parse(text).unwrap()
    }

    #[test]
    fn literals_canonicalize_as_postgresql_does() {
        assert_eq!(lit(Kind::Int4, "{}"), "{}");
        assert_eq!(lit(Kind::Int4, "{[1,2)}"), "{[1,2)}");
        assert_eq!(lit(Kind::Int4, "{[1,2),[3,4)}"), "{[1,2),[3,4)}");
        // Sorted, and overlapping or adjacent ranges join.
        assert_eq!(lit(Kind::Int4, "{[3,4),[1,2)}"), "{[1,2),[3,4)}");
        assert_eq!(lit(Kind::Int4, "{[1,3),[2,4)}"), "{[1,4)}");
        assert_eq!(lit(Kind::Int4, "{[1,2),[2,3)}"), "{[1,3)}");
        assert_eq!(lit(Kind::Int4, "{ [1,2) , [3,4) }"), "{[1,2),[3,4)}");
        assert_eq!(lit(Kind::Int4, "{empty}"), "{}");
        assert_eq!(lit(Kind::Int4, "{(,2),[3,)}"), "{(,2),[3,)}");
        assert_eq!(
            lit(
                Kind::Timestamp,
                r#"{["2020-01-01 00:00:00","2020-01-02 00:00:00")}"#
            ),
            r#"{["2020-01-01 00:00:00","2020-01-02 00:00:00")}"#
        );
        let error = from_literal(Kind::Int4, "x").unwrap_err();
        let message = crate::error_message(&error);
        assert!(
            message.contains("malformed multirange literal"),
            "{message}"
        );
        assert!(from_literal(Kind::Int4, "[1,2)").is_err());
        assert!(from_literal(Kind::Int4, "{[1,2)").is_err());
        assert!(from_literal(Kind::Int4, "{[1,2)}x").is_err());
        assert!(from_literal(Kind::Int4, "{[1,2),}").is_err());
        assert!(from_literal(Kind::Int4, "{{[1,2)}}").is_err());
        assert!(from_literal(Kind::Int4, "{[1,x)}").is_err());
    }

    #[test]
    fn multiranges_compare_and_operate() {
        let op = |op: &str, a: &str, b: &str| operator(op, Kind::Int4, a, b, false).unwrap();
        assert_eq!(op("@>", "{[1,2),[3,5)}", "[3,4)"), Value::Bool(true));
        assert_eq!(op("@>", "{[1,2),[3,5)}", "[1,4)"), Value::Bool(false));
        assert_eq!(op("@>", "{[1,2),[3,5)}", "{[3,4)}"), Value::Bool(true));
        assert_eq!(op("@>", "{[1,2),[3,5)}", "{}"), Value::Bool(true));
        assert_eq!(op("&&", "{[1,2)}", "{[2,3)}"), Value::Bool(false));
        assert_eq!(op("&&", "{[1,2)}", "{[1,2)}"), Value::Bool(true));
        assert_eq!(op("<<", "{[1,2)}", "{[3,4)}"), Value::Bool(true));
        assert_eq!(op("<<", "{[1,2)}", "{}"), Value::Bool(false));
        // A range on the other side of the family's operators.
        assert_eq!(op("&&", "[1,2)", "{[1,2),[3,5)}"), Value::Bool(true));
        assert_eq!(op("@>", "[1,5)", "{[1,2),[3,5)}"), Value::Bool(true));
        assert_eq!(op("@>", "[3,4)", "{[1,2),[3,5)}"), Value::Bool(false));
        assert_eq!(op("-|-", "{[1,2),[3,5)}", "[5,6)"), Value::Bool(true));
        assert_eq!(op("<<", "[1,2)", "{[3,5)}"), Value::Bool(true));
        assert_eq!(op("&<", "[1,4)", "{[1,2),[3,5)}"), Value::Bool(true));
        assert_eq!(op("&<", "[1,6)", "{[1,2),[3,5)}"), Value::Bool(false));
        assert_eq!(op("+", "{[1,2)}", "{[2,3)}"), Value::Text("{[1,3)}".into()));
        assert_eq!(op("*", "{[1,4)}", "{[2,5)}"), Value::Text("{[2,4)}".into()));
        assert_eq!(
            op("-", "{[1,4)}", "{[2,3)}"),
            Value::Text("{[1,2),[3,4)}".into())
        );
        assert_eq!(
            op("-", "{[1,5)}", "{[2,3),[4,5)}"),
            Value::Text("{[1,2),[3,4)}".into())
        );
        assert_eq!(op("=", "{[1,2)}", "{[1,2)}"), Value::Bool(true));
        assert_eq!(
            cmp(Kind::Int4, &elements("{}"), &elements("{[1,2)}")),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            cmp(Kind::Int4, &elements("{[1,2)}"), &elements("{[1,2),[3,4)}")),
            std::cmp::Ordering::Less
        );
        // `@>` with an element follows the element rule of its range.
        assert_eq!(
            operator("@>", Kind::Int4, "{[1,2),[3,5)}", "3", true).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            operator("@>", Kind::Int4, "{[1,2),[3,5)}", "5", true).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn multirange_functions_match_postgresql() {
        assert_eq!(
            bound_value("{[1,2),[3,5)}", true).unwrap(),
            Value::Text("1".into())
        );
        assert_eq!(
            bound_value("{[1,2),[3,5)}", false).unwrap(),
            Value::Text("5".into())
        );
        assert_eq!(bound_value("{}", true).unwrap(), Value::Null);
        assert_eq!(bound_inc("{[1,2),[3,5)}", true).unwrap(), Value::Bool(true));
        assert_eq!(bound_inc("{}", true).unwrap(), Value::Bool(false));
        assert_eq!(bound_inf("{(,2)}", true).unwrap(), Value::Bool(true));
        assert_eq!(is_empty("{}").unwrap(), Value::Bool(true));
        assert_eq!(
            range_merge("{[1,2),[3,5)}").unwrap(),
            Value::Text("[1,5)".into())
        );
        assert_eq!(range_merge("{}").unwrap(), Value::Text("empty".into()));
        assert_eq!(
            unnest("{[1,2),[3,5)}").unwrap(),
            vec![Value::Text("[1,2)".into()), Value::Text("[3,5)".into())]
        );
        assert_eq!(unnest("{}").unwrap(), Vec::<Value>::new());
        assert_eq!(
            merge_ranges(
                Kind::Int4,
                &[
                    "[1,3)".to_string(),
                    "[2,4)".to_string(),
                    "[6,7)".to_string()
                ]
            ),
            "{[1,4),[6,7)}"
        );
        assert_eq!(
            intersect_all(
                Kind::Int4,
                &["[1,5)".to_string(), "[2,4)".to_string()],
                false
            ),
            Value::Text("[2,4)".into())
        );
        assert_eq!(
            intersect_all(
                Kind::Int4,
                &["{[1,5)}".to_string(), "{[2,4)}".to_string()],
                true
            ),
            Value::Text("{[2,4)}".into())
        );
    }
}
