//! Range types: PostgreSQL's canonical text forms, constructors, accessors,
//! and operators. Values stay canonical text, like intervals and timestamps;
//! the declared type makes the text a range and picks the subtype.

use crate::Value;

/// A range type's subtype.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Kind {
    Int4,
    Int8,
    Numeric,
    Date,
    Timestamp,
    TimestampTz,
}

impl Kind {
    /// The range type a declared type names (`int4range`, `pg_catalog
    /// .daterange`), or `None`.
    pub(crate) fn of(data_type: &str) -> Option<Kind> {
        let upper = data_type.trim().to_ascii_uppercase();
        let name = upper.rsplit_once('.').map_or(upper.as_str(), |(_, n)| n);
        Some(match name.trim().trim_matches('"') {
            "INT4RANGE" => Kind::Int4,
            "INT8RANGE" => Kind::Int8,
            "NUMRANGE" => Kind::Numeric,
            "DATERANGE" => Kind::Date,
            "TSRANGE" => Kind::Timestamp,
            "TSTZRANGE" => Kind::TimestampTz,
            _ => return None,
        })
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Kind::Int4 => "int4range",
            Kind::Int8 => "int8range",
            Kind::Numeric => "numrange",
            Kind::Date => "daterange",
            Kind::Timestamp => "tsrange",
            Kind::TimestampTz => "tstzrange",
        }
    }

    pub(crate) fn oid(self) -> i64 {
        match self {
            Kind::Int4 => 3904,
            Kind::Int8 => 3926,
            Kind::Numeric => 3906,
            Kind::Date => 3912,
            Kind::Timestamp => 3908,
            Kind::TimestampTz => 3910,
        }
    }

    pub(crate) fn array_oid(self) -> i64 {
        self.oid() + 1
    }

    /// The range type an OID names, for `pg_type` and `format_type`.
    pub(crate) fn of_oid(oid: i64) -> Option<Kind> {
        [
            Kind::Int4,
            Kind::Int8,
            Kind::Numeric,
            Kind::Date,
            Kind::Timestamp,
            Kind::TimestampTz,
        ]
        .into_iter()
        .find(|k| k.oid() == oid)
    }

    /// The declared name of the subtype, for casting each bound.
    pub(crate) fn subtype(self) -> &'static str {
        match self {
            Kind::Int4 => "INT4",
            Kind::Int8 => "INT8",
            Kind::Numeric => "NUMERIC",
            Kind::Date => "DATE",
            Kind::Timestamp => "TIMESTAMP",
            Kind::TimestampTz => "TIMESTAMPTZ",
        }
    }

    pub(crate) fn subtype_oid(self) -> i64 {
        match self {
            Kind::Int4 => 23,
            Kind::Int8 => 20,
            Kind::Numeric => 1700,
            Kind::Date => 1082,
            Kind::Timestamp => 1114,
            Kind::TimestampTz => 1184,
        }
    }

    /// A discrete subtype's ranges are canonicalized to `[a, b)`: one unit
    /// past an inclusive lower bound and past an inclusive upper bound.
    pub(crate) fn discrete(self) -> bool {
        matches!(self, Kind::Int4 | Kind::Int8 | Kind::Date)
    }
}

/// Whether a declared type names a range type.
pub(crate) fn is_range_type(data_type: &str) -> bool {
    Kind::of(data_type).is_some()
}

/// The B-tree operator class `pg_range.rngsubopc` names, as PostgreSQL's
/// values.
pub(crate) fn subopc(kind: Kind) -> i64 {
    match kind {
        Kind::Int4 => 1978,
        Kind::Int8 => 3124,
        Kind::Numeric => 3125,
        Kind::Date => 3122,
        Kind::Timestamp => 3128,
        Kind::TimestampTz => 3127,
    }
}

/// The canonicalization function a discrete subtype has
/// (`pg_range.rngcanonical`).
pub(crate) fn canonical_function(kind: Kind) -> Option<&'static str> {
    kind.discrete().then(|| match kind {
        Kind::Int4 => "int4range_canonical",
        Kind::Int8 => "int8range_canonical",
        Kind::Date => "daterange_canonical",
        _ => unreachable!("only the discrete kinds are canonicalized"),
    })
}

/// Every range type, in OID order, for the catalogs.
pub(crate) const KINDS: [Kind; 6] = [
    Kind::Int4,
    Kind::Int8,
    Kind::Numeric,
    Kind::Date,
    Kind::Timestamp,
    Kind::TimestampTz,
];

impl Kind {
    /// The multirange type over this subtype (`int4multirange`).
    pub(crate) fn multirange_name(self) -> &'static str {
        match self {
            Kind::Int4 => "int4multirange",
            Kind::Int8 => "int8multirange",
            Kind::Numeric => "nummultirange",
            Kind::Date => "datemultirange",
            Kind::Timestamp => "tsmultirange",
            Kind::TimestampTz => "tstzmultirange",
        }
    }

    pub(crate) fn multirange_oid(self) -> i64 {
        match self {
            Kind::Int4 => 4451,
            Kind::Int8 => 4536,
            Kind::Numeric => 4532,
            Kind::Date => 4535,
            Kind::Timestamp => 4533,
            Kind::TimestampTz => 4534,
        }
    }

    pub(crate) fn multirange_array_oid(self) -> i64 {
        match self {
            Kind::Int4 => 6150,
            Kind::Int8 => 6157,
            Kind::Numeric => 6151,
            Kind::Date => 6155,
            Kind::Timestamp => 6152,
            Kind::TimestampTz => 6153,
        }
    }

    /// `pg_type.typalign` of the multirange type.
    pub(crate) fn multirange_align(self) -> char {
        match self {
            Kind::Int4 | Kind::Numeric | Kind::Date => 'i',
            Kind::Int8 | Kind::Timestamp | Kind::TimestampTz => 'd',
        }
    }

    /// The range subtype a declared multirange type names.
    pub(crate) fn of_multirange(data_type: &str) -> Option<Kind> {
        let upper = data_type.trim().to_ascii_uppercase();
        let name = upper.rsplit_once('.').map_or(upper.as_str(), |(_, n)| n);
        KINDS
            .into_iter()
            .find(|kind| kind.multirange_name().to_ascii_uppercase() == name.trim_matches('"'))
    }

    /// The multirange type an OID names, for `pg_type` and `format_type`.
    pub(crate) fn of_multirange_oid(oid: i64) -> Option<Kind> {
        KINDS.into_iter().find(|kind| kind.multirange_oid() == oid)
    }
}

/// A range's bound: its value as canonical subtype text, or none when
/// unbounded, and whether the bound is included.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Bound {
    pub(crate) value: Option<String>,
    pub(crate) inclusive: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Range {
    Empty,
    Bounds { lower: Bound, upper: Bound },
}

fn malformed(text: &str, detail: &str) -> String {
    crate::error_fields::DbError::new(format!("malformed range literal: \"{text}\""))
        .code("22P02")
        .detail(detail)
        .into_text()
}

fn not_contiguous(what: &str) -> String {
    crate::error_fields::DbError::new(format!("result of range {what} would not be contiguous"))
        .code("22000")
        .into_text()
}

/// The subtype's own reading of a bound's text (`invalid input syntax for
/// type integer: "x"` and the like).
pub(crate) fn typed(kind: Kind, raw: &str) -> Result<String, String> {
    crate::planner::try_cast(Value::Text(raw.to_string()), kind.subtype()).map(
        |value| match value {
            Value::Text(text) => text,
            other => crate::render(&other),
        },
    )
}

/// The value after `v`, for the discrete types' canonicalization.
fn next_value(kind: Kind, value: &str) -> Result<String, String> {
    match kind {
        Kind::Int4 | Kind::Int8 => {
            let n: i64 = value.parse().map_err(|_| {
                format!(
                    "invalid input syntax for type {}: \"{value}\"",
                    kind.subtype().to_ascii_lowercase()
                )
            })?;
            let next = n.checked_add(1).ok_or_else(|| {
                crate::error_fields::DbError::new("bigint out of range")
                    .code("22003")
                    .into_text()
            })?;
            if kind == Kind::Int4 && next > i64::from(i32::MAX) {
                return Err(crate::error_fields::DbError::new("integer out of range")
                    .code("22003")
                    .into_text());
            }
            Ok(next.to_string())
        }
        Kind::Date => {
            let date = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .map_err(|_| format!("invalid input syntax for type date: \"{value}\""))?;
            let next = date.succ_opt().ok_or_else(|| {
                crate::error_fields::DbError::new("date out of range")
                    .code("22008")
                    .into_text()
            })?;
            Ok(next.format("%Y-%m-%d").to_string())
        }
        _ => Ok(value.to_string()),
    }
}

/// The comparison of two bound values of the same subtype: numeric subtypes
/// compare by value, and canonical date and timestamp text chronologically.
pub(crate) fn cmp_value(kind: Kind, a: &str, b: &str) -> std::cmp::Ordering {
    match kind {
        Kind::Int4 | Kind::Int8 => match (a.parse::<i64>(), b.parse::<i64>()) {
            (Ok(x), Ok(y)) => x.cmp(&y),
            _ => a.cmp(b),
        },
        Kind::Numeric => match (
            crate::value::parse_decimal(a),
            crate::value::parse_decimal(b),
        ) {
            (Some(x), Some(y)) => crate::value::compare(&Value::Numeric(x), &Value::Numeric(y)),
            _ => a.cmp(b),
        },
        // Canonical date and timestamp text sorts chronologically.
        _ => a.cmp(b),
    }
}

/// Two bounds by value, then by inclusion: an inclusive bound is the larger,
/// so `[1` starts before `(1` and `1]` ends after `1)`.
fn cmp_bounds(kind: Kind, a: &Bound, b: &Bound) -> std::cmp::Ordering {
    match (&a.value, &b.value) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(x), Some(y)) => cmp_value(kind, x, y).then(a.inclusive.cmp(&b.inclusive)),
    }
}

/// A bound pair in PostgreSQL's bracket notation, canonicalized: an
/// exclusive finite lower bound and an inclusive finite upper bound move one
/// unit for the discrete subtypes, and an empty result is `empty`.
fn canonical(kind: Kind, mut lower: Bound, mut upper: Bound) -> Result<String, String> {
    if kind.discrete() {
        if let Some(value) = lower.value.clone()
            && !lower.inclusive
        {
            lower = Bound {
                value: Some(next_value(kind, &value)?),
                inclusive: true,
            };
        }
        if let Some(value) = upper.value.clone()
            && upper.inclusive
        {
            upper = Bound {
                value: Some(next_value(kind, &value)?),
                inclusive: false,
            };
        }
    }
    if let (Some(l), Some(u)) = (&lower.value, &upper.value) {
        match cmp_value(kind, l, u) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal if !(lower.inclusive && upper.inclusive) => {
                return Ok("empty".to_string());
            }
            std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Greater => {
                return Err(crate::error_fields::DbError::new(
                    "range lower bound must be less than or equal to range upper bound",
                )
                .code("22000")
                .into_text());
            }
        }
    }
    Ok(format_range(&Range::Bounds { lower, upper }))
}

/// A bound value quoted as PostgreSQL writes it.
fn quote_bound(value: &str) -> String {
    let needs_quotes = value.is_empty()
        || value
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, ',' | '[' | ']' | '(' | ')' | '"' | '\\'));
    if needs_quotes {
        format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        value.to_string()
    }
}

/// A range as PostgreSQL writes it: brackets, each bound quoted when it
/// needs it.
pub(crate) fn format_range(range: &Range) -> String {
    let Range::Bounds { lower, upper } = range else {
        return "empty".to_string();
    };
    let mut out = String::new();
    out.push(if lower.inclusive && lower.value.is_some() {
        '['
    } else {
        '('
    });
    if let Some(value) = &lower.value {
        out.push_str(&quote_bound(value));
    }
    out.push(',');
    if let Some(value) = &upper.value {
        out.push_str(&quote_bound(value));
    }
    out.push(if upper.inclusive && upper.value.is_some() {
        ']'
    } else {
        ')'
    });
    out
}

/// Splits `1,5` and `"a,b",c` at the top-level comma; `None` without one.
fn split_bounds(inner: &str) -> Option<(Option<String>, Option<String>)> {
    let mut parts: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                current.push('"');
                loop {
                    match chars.next() {
                        Some('\\') => {
                            current.push('\\');
                            current.push(chars.next()?);
                        }
                        Some('"') => {
                            current.push('"');
                            break;
                        }
                        Some(other) => current.push(other),
                        None => return None,
                    }
                }
            }
            ',' => {
                parts.push(std::mem::take(&mut current));
                if parts.len() > 1 {
                    return None;
                }
            }
            other => current.push(other),
        }
    }
    parts.push(current);
    let [lower, upper] = parts.as_slice() else {
        return None;
    };
    let bound = |raw: &str| -> Option<String> {
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        if let Some(quoted) = raw.strip_prefix('"') {
            // The scanner kept the quotes; unfold the escapes.
            let quoted = quoted.strip_suffix('"')?;
            let mut out = String::new();
            let mut chars = quoted.chars();
            while let Some(c) = chars.next() {
                match c {
                    '\\' => out.push(chars.next()?),
                    other => out.push(other),
                }
            }
            return Some(out);
        }
        Some(raw.to_string())
    };
    Some((bound(lower), bound(upper)))
}

/// Parses canonical range text back into its bounds; `empty` is
/// [`Range::Empty`].
pub(crate) fn parse(text: &str) -> Result<Range, String> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("empty") {
        return Ok(Range::Empty);
    }
    let chars: Vec<char> = trimmed.chars().collect();
    let lower_inc = match chars.first() {
        Some('[') => true,
        Some('(') => false,
        _ => return Err(malformed(trimmed, "Missing left parenthesis or bracket.")),
    };
    let upper_inc = match chars.last() {
        Some(']') => true,
        Some(')') => false,
        _ => return Err(malformed(trimmed, "Unexpected end of input.")),
    };
    let inner: String = chars[1..chars.len().saturating_sub(1)].iter().collect();
    let Some((lower, upper)) = split_bounds(&inner) else {
        return Err(malformed(trimmed, "Missing comma after lower bound."));
    };
    let bound = |value: Option<String>, inclusive: bool| Bound {
        inclusive: value.is_some() && inclusive,
        value,
    };
    Ok(Range::Bounds {
        lower: bound(lower, lower_inc),
        upper: bound(upper, upper_inc),
    })
}

/// A literal as the type takes it: parsed, typed, and canonical.
pub(crate) fn from_literal(kind: Kind, text: &str) -> Result<String, String> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("empty") {
        return Ok("empty".to_string());
    }
    let chars: Vec<char> = trimmed.chars().collect();
    let lower_inc = match chars.first() {
        Some('[') => true,
        Some('(') => false,
        _ => return Err(malformed(trimmed, "Missing left parenthesis or bracket.")),
    };
    let upper_inc = match chars.last() {
        Some(']') => true,
        Some(')') => false,
        _ => return Err(malformed(trimmed, "Unexpected end of input.")),
    };
    let inner: String = chars[1..chars.len().saturating_sub(1)].iter().collect();
    let Some((lower, upper)) = split_bounds(&inner) else {
        return Err(malformed(trimmed, "Missing comma after lower bound."));
    };
    let typed_bound = |raw: Option<String>, inclusive: bool| -> Result<Bound, String> {
        let value = match raw {
            None => None,
            Some(raw) => Some(typed(kind, &raw)?),
        };
        Ok(Bound {
            value: value.clone(),
            inclusive: value.is_some() && inclusive,
        })
    };
    canonical(
        kind,
        typed_bound(lower, lower_inc)?,
        typed_bound(upper, upper_inc)?,
    )
}

/// The constructor `int4range(lower, upper [, flags])`.
pub(crate) fn from_bounds(
    kind: Kind,
    lower: Option<&Value>,
    upper: Option<&Value>,
    flags: &str,
) -> Result<String, String> {
    let (lower_inc, upper_inc) = match flags {
        "[]" => (true, true),
        "[)" => (true, false),
        "(]" => (false, true),
        "()" => (false, false),
        _ => {
            return Err(
                crate::error_fields::DbError::new("invalid range bound flags")
                    .code("22023")
                    .hint("Valid values are \"[]\", \"[)\", \"(]\", and \"()\".")
                    .into_text(),
            );
        }
    };
    let typed_bound = |value: Option<&Value>, inclusive: bool| -> Result<Bound, String> {
        let value = match value {
            None | Some(Value::Null) => None,
            Some(Value::Text(text)) => Some(typed(kind, text)?),
            Some(other) => Some(typed(kind, &crate::render(other))?),
        };
        Ok(Bound {
            value: value.clone(),
            inclusive: value.is_some() && inclusive,
        })
    };
    canonical(
        kind,
        typed_bound(lower, lower_inc)?,
        typed_bound(upper, upper_inc)?,
    )
}

/// `lower(range)` and `upper(range)`: the bound's text, or NULL.
pub(crate) fn bound_value(text: &str, lower: bool) -> Result<Value, String> {
    match parse(text)? {
        Range::Empty => Ok(Value::Null),
        Range::Bounds { lower: l, upper: u } => {
            let bound = if lower { l } else { u };
            Ok(bound.value.map_or(Value::Null, Value::Text))
        }
    }
}

/// `lower_inc(range)` and `upper_inc(range)`: empty and unbounded bounds are
/// not included.
pub(crate) fn bound_inc(text: &str, lower: bool) -> Result<Value, String> {
    match parse(text)? {
        Range::Empty => Ok(Value::Bool(false)),
        Range::Bounds { lower: l, upper: u } => {
            let bound = if lower { l } else { u };
            Ok(Value::Bool(bound.value.is_some() && bound.inclusive))
        }
    }
}

/// `lower_inf(range)` and `upper_inf(range)`.
pub(crate) fn bound_inf(text: &str, lower: bool) -> Result<Value, String> {
    match parse(text)? {
        Range::Empty => Ok(Value::Bool(false)),
        Range::Bounds { lower: l, upper: u } => {
            let bound = if lower { l } else { u };
            Ok(Value::Bool(bound.value.is_none()))
        }
    }
}

/// `isempty(range)`.
pub(crate) fn is_empty(text: &str) -> Result<Value, String> {
    Ok(Value::Bool(matches!(parse(text)?, Range::Empty)))
}

/// The kind of two range texts whose declared type is not known, for
/// `range_merge` of untyped literals.
pub(crate) fn infer_kind(a: &str, b: &str) -> Option<Kind> {
    KINDS
        .into_iter()
        .find(|kind| from_literal(*kind, a).is_ok() && from_literal(*kind, b).is_ok())
}

/// The comparison of two ranges: `empty` sorts first, then the lower bounds,
/// then the upper bounds.
pub(crate) fn cmp_ranges(kind: Kind, a: &Range, b: &Range) -> std::cmp::Ordering {
    match (a, b) {
        (Range::Empty, Range::Empty) => std::cmp::Ordering::Equal,
        (Range::Empty, _) => std::cmp::Ordering::Less,
        (_, Range::Empty) => std::cmp::Ordering::Greater,
        (
            Range::Bounds {
                lower: l1,
                upper: u1,
            },
            Range::Bounds {
                lower: l2,
                upper: u2,
            },
        ) => cmp_bounds(kind, l1, l2).then(cmp_bounds(kind, u1, u2)),
    }
}

/// Whether `outer` contains `inner`.
pub(crate) fn contains_range(kind: Kind, outer: &Range, inner: &Range) -> bool {
    let (
        Range::Bounds {
            lower: l1,
            upper: u1,
        },
        Range::Bounds {
            lower: l2,
            upper: u2,
        },
    ) = (outer, inner)
    else {
        return false;
    };
    let lower_ok = match (&l1.value, &l2.value) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(a), Some(b)) => match cmp_value(kind, a, b) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Equal => l1.inclusive || !l2.inclusive,
            std::cmp::Ordering::Greater => false,
        },
    };
    let upper_ok = match (&u1.value, &u2.value) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(a), Some(b)) => match cmp_value(kind, a, b) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Equal => u1.inclusive || !u2.inclusive,
            std::cmp::Ordering::Less => false,
        },
    };
    lower_ok && upper_ok
}

/// Whether a range contains an element.
pub(crate) fn contains_element(kind: Kind, range: &Range, element: &str) -> bool {
    let Range::Bounds { lower, upper } = range else {
        return false;
    };
    let lower_ok = match &lower.value {
        None => true,
        Some(a) => match cmp_value(kind, element, a) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Equal => lower.inclusive,
            std::cmp::Ordering::Less => false,
        },
    };
    let upper_ok = match &upper.value {
        None => true,
        Some(a) => match cmp_value(kind, element, a) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Equal => upper.inclusive,
            std::cmp::Ordering::Greater => false,
        },
    };
    lower_ok && upper_ok
}

/// Whether two ranges share any element.
pub(crate) fn overlaps(kind: Kind, a: &Range, b: &Range) -> bool {
    let (
        Range::Bounds {
            lower: l1,
            upper: u1,
        },
        Range::Bounds {
            lower: l2,
            upper: u2,
        },
    ) = (a, b)
    else {
        return false;
    };
    // Each range's lower bound is below the other's upper bound.
    bound_below(kind, l1, u2) && bound_below(kind, l2, u1)
}

/// Whether bound `a` (a lower bound) lies below bound `b` (an upper bound):
/// an unbound lower bound is below everything and an unbound upper bound
/// above everything, and equal values meet only when both are inclusive.
fn bound_below(kind: Kind, a: &Bound, b: &Bound) -> bool {
    match (&a.value, &b.value) {
        (None, _) => true,
        (Some(_), None) => true,
        (Some(x), Some(y)) => match cmp_value(kind, x, y) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Equal => a.inclusive && b.inclusive,
            std::cmp::Ordering::Greater => false,
        },
    }
}

/// Whether `a` lies strictly left of `b` (its upper bound at or below `b`'s
/// lower bound).
pub(crate) fn strictly_left(kind: Kind, a: &Range, b: &Range) -> bool {
    let (Range::Bounds { upper: u1, .. }, Range::Bounds { lower: l2, .. }) = (a, b) else {
        return false;
    };
    cmp_bounds(kind, u1, l2) != std::cmp::Ordering::Greater
}

/// Whether `a` lies strictly right of `b`.
pub(crate) fn strictly_right(kind: Kind, a: &Range, b: &Range) -> bool {
    let (Range::Bounds { lower: l1, .. }, Range::Bounds { upper: u2, .. }) = (a, b) else {
        return false;
    };
    cmp_bounds(kind, l1, u2) != std::cmp::Ordering::Less
}

/// Whether `a` does not extend to the right of `b`.
pub(crate) fn not_right_of(kind: Kind, a: &Range, b: &Range) -> bool {
    let (Range::Bounds { upper: u1, .. }, Range::Bounds { upper: u2, .. }) = (a, b) else {
        return false;
    };
    cmp_bounds(kind, u1, u2) != std::cmp::Ordering::Greater
}

/// Whether `a` does not extend to the left of `b`.
pub(crate) fn not_left_of(kind: Kind, a: &Range, b: &Range) -> bool {
    let (Range::Bounds { lower: l1, .. }, Range::Bounds { lower: l2, .. }) = (a, b) else {
        return false;
    };
    cmp_bounds(kind, l1, l2) != std::cmp::Ordering::Less
}

/// Whether the ranges touch: an upper bound equal to a lower bound with
/// exactly one of them inclusive.
pub(crate) fn adjacent(kind: Kind, a: &Range, b: &Range) -> bool {
    let (Range::Bounds { upper: u1, .. }, Range::Bounds { lower: l2, .. }) = (a, b) else {
        return false;
    };
    match (&u1.value, &l2.value) {
        (Some(x), Some(y)) => {
            cmp_value(kind, x, y) == std::cmp::Ordering::Equal && u1.inclusive != l2.inclusive
        }
        _ => false,
    }
}

/// The union of two contiguous ranges.
pub(crate) fn union(kind: Kind, a: &Range, b: &Range) -> Result<Range, String> {
    match (a, b) {
        (Range::Empty, other) | (other, Range::Empty) => Ok(other.clone()),
        (
            Range::Bounds {
                lower: l1,
                upper: u1,
            },
            Range::Bounds {
                lower: l2,
                upper: u2,
            },
        ) => {
            if !overlaps(kind, a, b) && !adjacent(kind, a, b) && !adjacent(kind, b, a) {
                return Err(not_contiguous("union"));
            }
            let lower = if cmp_bounds(kind, l1, l2) != std::cmp::Ordering::Greater {
                l1.clone()
            } else {
                l2.clone()
            };
            let upper = if cmp_bounds(kind, u1, u2) != std::cmp::Ordering::Less {
                u1.clone()
            } else {
                u2.clone()
            };
            Ok(Range::Bounds { lower, upper })
        }
    }
}

/// The intersection of two ranges (`empty` when they do not overlap).
pub(crate) fn intersection(kind: Kind, a: &Range, b: &Range) -> Range {
    if !overlaps(kind, a, b) {
        return Range::Empty;
    }
    let (
        Range::Bounds {
            lower: l1,
            upper: u1,
        },
        Range::Bounds {
            lower: l2,
            upper: u2,
        },
    ) = (a, b)
    else {
        return Range::Empty;
    };
    let lower = if cmp_bounds(kind, l1, l2) != std::cmp::Ordering::Less {
        l1.clone()
    } else {
        l2.clone()
    };
    let upper = if cmp_bounds(kind, u1, u2) != std::cmp::Ordering::Greater {
        u1.clone()
    } else {
        u2.clone()
    };
    if cmp_bounds(kind, &lower, &upper) == std::cmp::Ordering::Greater {
        Range::Empty
    } else {
        Range::Bounds { lower, upper }
    }
}

/// The difference of two ranges, which is a range only when a single piece
/// remains.
pub(crate) fn difference(kind: Kind, a: &Range, b: &Range) -> Result<Range, String> {
    match (a, b) {
        (Range::Empty, _) => Ok(Range::Empty),
        (_, Range::Empty) => Ok(a.clone()),
        _ if contains_range(kind, b, a) => Ok(Range::Empty),
        _ if !overlaps(kind, a, b) => Ok(a.clone()),
        (
            Range::Bounds {
                lower: l1,
                upper: u1,
            },
            Range::Bounds {
                lower: l2,
                upper: u2,
            },
        ) => {
            // `b` cuts a piece off `a`'s lower or upper end; both ends is a
            // hole, which is not a range.
            let cuts_low = cmp_bounds(kind, l2, l1) != std::cmp::Ordering::Greater;
            let cuts_high = cmp_bounds(kind, u2, u1) != std::cmp::Ordering::Less;
            match (cuts_low, cuts_high) {
                (true, false) => Ok(Range::Bounds {
                    lower: Bound {
                        value: u2.value.clone(),
                        inclusive: u2.value.is_some() && !u2.inclusive,
                    },
                    upper: u1.clone(),
                }),
                (false, true) => Ok(Range::Bounds {
                    lower: l1.clone(),
                    upper: Bound {
                        value: l2.value.clone(),
                        inclusive: l2.value.is_some() && !l2.inclusive,
                    },
                }),
                _ => Err(not_contiguous("difference")),
            }
        }
    }
}

/// The hull of two ranges (`range_merge` and the merge of non-contiguous
/// ranges).
fn merge(kind: Kind, a: &Range, b: &Range) -> Result<Range, String> {
    match (a, b) {
        (Range::Empty, other) | (other, Range::Empty) => Ok(other.clone()),
        (
            Range::Bounds {
                lower: l1,
                upper: u1,
            },
            Range::Bounds {
                lower: l2,
                upper: u2,
            },
        ) => {
            let lower = if cmp_bounds(kind, l1, l2) != std::cmp::Ordering::Greater {
                l1.clone()
            } else {
                l2.clone()
            };
            let upper = if cmp_bounds(kind, u1, u2) != std::cmp::Ordering::Less {
                u1.clone()
            } else {
                u2.clone()
            };
            Ok(Range::Bounds { lower, upper })
        }
    }
}

/// `range_merge(a, b)`: the smallest range covering both.
pub(crate) fn range_merge(kind: Kind, a: &str, b: &str) -> Result<Value, String> {
    let merged = merge(kind, &canonical_range(kind, a)?, &canonical_range(kind, b)?)?;
    Ok(Value::Text(format_range(&merged)))
}

/// A range operand as canonical text read back into its bounds.
pub(crate) fn canonical_range(kind: Kind, text: &str) -> Result<Range, String> {
    parse(&from_literal(kind, text)?)
}

/// A range operator's result. `right_is_element` distinguishes
/// `range @> element` from `range @> range`; `<@` arrives with its operands
/// swapped.
pub(crate) fn operator(
    op: &str,
    kind: Kind,
    left: &str,
    right: &str,
    right_is_element: bool,
) -> Result<Value, String> {
    let a = canonical_range(kind, left)?;
    let result = match op {
        "@>" => {
            let holds = if right_is_element {
                contains_element(kind, &a, &typed(kind, right)?)
            } else {
                contains_range(kind, &a, &canonical_range(kind, right)?)
            };
            Value::Bool(holds)
        }
        "&&" => Value::Bool(overlaps(kind, &a, &canonical_range(kind, right)?)),
        "<<" => Value::Bool(strictly_left(kind, &a, &canonical_range(kind, right)?)),
        ">>" => Value::Bool(strictly_right(kind, &a, &canonical_range(kind, right)?)),
        "&<" => Value::Bool(not_right_of(kind, &a, &canonical_range(kind, right)?)),
        "&>" => Value::Bool(not_left_of(kind, &a, &canonical_range(kind, right)?)),
        "-|-" => Value::Bool(adjacent(kind, &a, &canonical_range(kind, right)?)),
        "+" => Value::Text(format_range(&union(
            kind,
            &a,
            &canonical_range(kind, right)?,
        )?)),
        "*" => Value::Text(format_range(&intersection(
            kind,
            &a,
            &canonical_range(kind, right)?,
        ))),
        "-" => Value::Text(format_range(&difference(
            kind,
            &a,
            &canonical_range(kind, right)?,
        )?)),
        "=" | "<>" | "<" | ">" | "<=" | ">=" => {
            let b = canonical_range(kind, right)?;
            let cmp = cmp_ranges(kind, &a, &b);
            use std::cmp::Ordering::*;
            Value::Bool(match op {
                "=" => cmp == Equal,
                "<>" => cmp != Equal,
                "<" => cmp == Less,
                ">" => cmp == Greater,
                "<=" => cmp != Greater,
                _ => cmp != Less,
            })
        }
        other => return Err(format!("unsupported range operator {other}")),
    };
    Ok(result)
}

/// An order-preserving key for sorting range values.
pub(crate) fn sort_key(kind: Kind, text: &str) -> String {
    let Ok(range) = parse(text) else {
        return text.to_string();
    };
    match range {
        Range::Empty => "0".to_string(),
        Range::Bounds { lower, upper } => {
            let bound_key = |bound: &Bound, is_lower: bool| match &bound.value {
                None if is_lower => "0".to_string(),
                None => "2".to_string(),
                Some(value) => {
                    let flag = if (is_lower && bound.inclusive) || (!is_lower && !bound.inclusive) {
                        '0'
                    } else {
                        '1'
                    };
                    format!("1{flag}{}", sortable(kind, value))
                }
            };
            format!("1{}{}", bound_key(&lower, true), bound_key(&upper, false))
        }
    }
}

/// A single bound value as text that sorts by the subtype's order.
fn sortable(kind: Kind, value: &str) -> String {
    match kind {
        Kind::Int4 | Kind::Int8 => {
            let n: i64 = value.parse().unwrap_or(0);
            format!("{:020}", (n as i128 + (1i128 << 63)) as u128)
        }
        Kind::Numeric => {
            // A numeric's exact order in a text key: the sign, then the
            // decimal exponent, then the digits.
            let decimal = crate::value::parse_decimal(value);
            match decimal {
                Some(d) => numeric_key(&d.to_string()),
                None => value.to_string(),
            }
        }
        // Canonical date and timestamp text sorts chronologically.
        _ => value.to_string(),
    }
}

/// A decimal's text as an order-preserving key: a sign, the position of the
/// first significant digit, then its digits.
fn numeric_key(text: &str) -> String {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (integer, fraction) = match digits.split_once('.') {
        Some((i, f)) => (i, f),
        None => (digits, ""),
    };
    let digits = format!("{integer}{fraction}");
    let first = digits
        .bytes()
        .position(|b| b != b'0')
        .unwrap_or(digits.len());
    if first == digits.len() {
        // Zero (and -0): between the negatives and the positives.
        return "1".to_string();
    }
    // The value is digits[first..] * 10^(exponent), with the decimal point
    // after the first `first` digits of the concatenation.
    let exponent = integer.len() as i64 - first as i64;
    let sign = if negative { '0' } else { '2' };
    // A larger exponent means a larger magnitude: offset for negatives. The
    // digits of a negative value are complemented, so they descend with the
    // value the way its sign reversed.
    let encoded = if negative {
        format!("{:010}", (1_000_000_000i64 - exponent).max(0))
    } else {
        format!("{:010}", exponent + 1_000_000_000)
    };
    let digits: String = if negative {
        digits[first..]
            .chars()
            .map(|c| (b'9' - (c as u8 - b'0')) as char)
            .collect()
    } else {
        digits[first..].to_string()
    };
    format!("{sign}{encoded}{digits}")
}

/// A `tstzrange` value as the session's zone shows it: each finite bound
/// moves from UTC to the session offset.
pub(crate) fn localize(text: &str) -> String {
    let Ok(range) = parse(text) else {
        return text.to_string();
    };
    let localize_bound = |bound: Bound| -> Bound {
        let Some(value) = &bound.value else {
            return bound;
        };
        Bound {
            value: Some(crate::timezone::session_timestamptz_text(value)),
            inclusive: bound.inclusive,
        }
    };
    match range {
        Range::Empty => "empty".to_string(),
        Range::Bounds { lower, upper } => format_range(&Range::Bounds {
            lower: localize_bound(lower),
            upper: localize_bound(upper),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit(kind: Kind, text: &str) -> String {
        from_literal(kind, text).unwrap()
    }

    fn message(error: &str) -> String {
        crate::error_message(error).to_string()
    }

    #[test]
    fn literals_canonicalize_as_postgresql_does() {
        assert_eq!(lit(Kind::Int4, "empty"), "empty");
        assert_eq!(lit(Kind::Int4, "[1,5)"), "[1,5)");
        assert_eq!(lit(Kind::Int4, "[1,5]"), "[1,6)");
        assert_eq!(lit(Kind::Int4, "(1,5)"), "[2,5)");
        assert_eq!(lit(Kind::Int4, "(1,5]"), "[2,6)");
        assert_eq!(lit(Kind::Int4, "[,5)"), "(,5)");
        assert_eq!(lit(Kind::Int4, "(,)"), "(,)");
        assert_eq!(lit(Kind::Int4, "[1,1)"), "empty");
        assert_eq!(
            lit(Kind::Date, "[2020-01-01,2020-01-03]"),
            "[2020-01-01,2020-01-04)"
        );
        assert_eq!(
            lit(Kind::Date, "(2020-01-01,2020-01-03)"),
            "[2020-01-02,2020-01-03)"
        );
        // Continuous subtypes keep their bounds and quote timestamps.
        assert_eq!(lit(Kind::Numeric, "(1.5,2.5]"), "(1.5,2.5]");
        assert_eq!(
            lit(Kind::Timestamp, "[2020-01-01 00:00:00,2020-01-02 00:00:00)"),
            "[\"2020-01-01 00:00:00\",\"2020-01-02 00:00:00\")"
        );
        assert_eq!(
            lit(
                Kind::TimestampTz,
                "[2020-01-01 00:00:00+00,2020-01-02 00:00:00+00)"
            ),
            "[\"2020-01-01 00:00:00+00\",\"2020-01-02 00:00:00+00\")"
        );
        // A quoted bound keeps its comma and escapes.
        assert_eq!(
            lit(
                Kind::Timestamp,
                "[\"2020-01-01 00:00:00\",\"2020-01-02 00:00:00\")"
            ),
            "[\"2020-01-01 00:00:00\",\"2020-01-02 00:00:00\")"
        );
    }

    #[test]
    fn literals_report_postgresql_errors() {
        assert!(
            message(&from_literal(Kind::Int4, "x").unwrap_err())
                .contains("malformed range literal")
        );
        assert!(
            message(&from_literal(Kind::Int4, "(1,5").unwrap_err())
                .contains("malformed range literal")
        );
        assert!(
            message(&from_literal(Kind::Int4, "[1,x)").unwrap_err())
                .contains("invalid input syntax for type integer")
        );
        assert!(
            message(
                &from_bounds(Kind::Int4, Some(&Value::Int(5)), Some(&Value::Int(1)), "[)")
                    .unwrap_err()
            )
            .contains("range lower bound must be less than or equal to range upper bound")
        );
        assert!(
            message(&from_bounds(Kind::Int4, None, None, "x").unwrap_err())
                .contains("invalid range bound flags")
        );
    }

    #[test]
    fn constructors_take_bounds_and_flags() {
        let bounds = |lower: Option<Value>, upper: Option<Value>, flags: &str| {
            from_bounds(Kind::Int4, lower.as_ref(), upper.as_ref(), flags).unwrap()
        };
        assert_eq!(
            bounds(Some(Value::Int(1)), Some(Value::Int(5)), "[)"),
            "[1,5)"
        );
        assert_eq!(
            bounds(Some(Value::Int(1)), Some(Value::Int(5)), "[]"),
            "[1,6)"
        );
        assert_eq!(bounds(Some(Value::Int(1)), None, "[)"), "[1,)");
        assert_eq!(bounds(None, Some(Value::Int(5)), "[)"), "(,5)");
        assert_eq!(
            bounds(Some(Value::Int(1)), Some(Value::Int(1)), "[)"),
            "empty"
        );
        assert_eq!(
            from_bounds(
                Kind::Numeric,
                Some(&Value::Text("1.5".into())),
                Some(&Value::Text("2.5".into())),
                "(]"
            )
            .unwrap(),
            "(1.5,2.5]"
        );
    }

    #[test]
    fn accessors_read_the_bounds() {
        let value = |f: fn(&str, bool) -> Result<Value, String>| f("[1,5)", true).unwrap();
        assert_eq!(value(bound_value), Value::Text("1".into()));
        assert_eq!(value(bound_inc), Value::Bool(true));
        assert_eq!(value(bound_inf), Value::Bool(false));
        assert_eq!(bound_value("empty", true).unwrap(), Value::Null);
        assert_eq!(bound_inc("empty", true).unwrap(), Value::Bool(false));
        assert_eq!(bound_inf("empty", true).unwrap(), Value::Bool(false));
        assert_eq!(bound_value("(,5)", true).unwrap(), Value::Null);
        assert_eq!(bound_inf("(,5)", true).unwrap(), Value::Bool(true));
        assert_eq!(is_empty("empty").unwrap(), Value::Bool(true));
        assert_eq!(is_empty("[1,2)").unwrap(), Value::Bool(false));
    }

    #[test]
    fn operators_follow_postgresql() {
        let op = |op: &str, a: &str, b: &str| operator(op, Kind::Int4, a, b, false).unwrap();
        assert_eq!(op("=", "[2,5)", "(1,5)"), Value::Bool(true));
        assert_eq!(op("<", "[1,5)", "[2,5)"), Value::Bool(true));
        assert_eq!(op("<", "empty", "[1,2)"), Value::Bool(true));
        assert_eq!(op("&&", "[1,5)", "[4,8)"), Value::Bool(true));
        assert_eq!(op("&&", "[1,5)", "[5,8)"), Value::Bool(false));
        assert_eq!(op("&&", "[1,5)", "empty"), Value::Bool(false));
        assert_eq!(op("<<", "[1,5)", "[5,8)"), Value::Bool(true));
        assert_eq!(op(">>", "[1,5)", "[5,8)"), Value::Bool(false));
        assert_eq!(op("&<", "[1,5)", "[2,3)"), Value::Bool(false));
        assert_eq!(op("&<", "[1,5)", "[5,8)"), Value::Bool(true));
        assert_eq!(op("&>", "[1,5)", "[4,9)"), Value::Bool(false));
        assert_eq!(op("-|-", "[1,5)", "[5,8)"), Value::Bool(true));
        assert_eq!(op("+", "[1,5)", "[5,8)"), Value::Text("[1,8)".into()));
        assert_eq!(op("+", "[1,5)", "empty"), Value::Text("[1,5)".into()));
        assert_eq!(op("*", "[1,5)", "[2,8)"), Value::Text("[2,5)".into()));
        assert_eq!(op("*", "[1,5)", "[6,8)"), Value::Text("empty".into()));
        assert_eq!(op("-", "[1,5)", "[1,2)"), Value::Text("[2,5)".into()));
        assert_eq!(op("-", "[1,5)", "[4,5)"), Value::Text("[1,4)".into()));
        let err = operator("+", Kind::Int4, "[1,5)", "[8,9)", false).unwrap_err();
        assert!(message(&err).contains("result of range union would not be contiguous"));
        let err = operator("-", Kind::Int4, "[1,5)", "[2,3)", false).unwrap_err();
        assert!(message(&err).contains("result of range difference would not be contiguous"));
        // Containment, of a range and of an element.
        assert_eq!(
            operator("@>", Kind::Int4, "[1,5)", "[2,3)", false).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            operator("@>", Kind::Int4, "[1,5)", "3", true).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            operator("@>", Kind::Int4, "[1,5)", "5", true).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            operator("@>", Kind::Int4, "[1,5]", "5", true).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            range_merge(Kind::Int4, "[1,2)", "[5,6)").unwrap(),
            Value::Text("[1,6)".into())
        );
    }

    #[test]
    fn sort_keys_put_empty_first_and_order_by_bounds() {
        let key = |text: &str| sort_key(Kind::Int4, text);
        assert!(key("empty") < key("[1,10)"));
        assert!(key("[1,10)") < key("[2,3)"));
        assert!(key("[1,5)") < key("[1,6)"));
        // Numeric keys follow the subtype's order, and an inclusive upper
        // bound is larger than an exclusive one.
        assert!(sort_key(Kind::Numeric, "[1,5)") < sort_key(Kind::Numeric, "[1,5]"));
        let key = |text: &str| sort_key(Kind::Numeric, text);
        assert!(key("[1.25,2.5)") < key("[1.5,2.5)"));
        assert!(key("[-2,1)") < key("[-1.5,1)"));
        assert!(key("[-1.5,1)") < key("[0,1)"));
        assert!(key("[-1,1)") < key("[-1.5,1)"));
    }
}
