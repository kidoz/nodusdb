//! Declarative table partitioning: `PARTITION BY`, partition bounds, and
//! the routing of a row to the partition its key falls in.
//!
//! The key and each partition's bound travel as the canonical text
//! PostgreSQL renders (`RANGE (id)`, `FOR VALUES FROM (1) TO (10)`,
//! `FOR VALUES IN ('a', 'b')`, `FOR VALUES WITH (modulus 4, remainder 0)`,
//! `DEFAULT`); this module parses them against the key columns' types and
//! matches values the way each strategy does — ranges by comparison, lists
//! by equality (NULL matching a NULL element), hashes by PostgreSQL's own
//! hash functions modulo the modulus.

use crate::Value;
use anyhow::Result;
use nodus_catalog::TableDescriptor;

/// A partitioned table's key: the strategy and its columns.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PartitionKey {
    pub(crate) strategy: Strategy,
    pub(crate) columns: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Strategy {
    Range,
    List,
    Hash,
}

impl Strategy {
    fn name(self) -> &'static str {
        match self {
            Strategy::Range => "RANGE",
            Strategy::List => "LIST",
            Strategy::Hash => "HASH",
        }
    }
}

/// One value of a range or list bound.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BoundValue {
    Min,
    Max,
    Value(Value),
}

/// A partition's bound, parsed from its canonical text.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Bound {
    Range {
        from: Vec<BoundValue>,
        to: Vec<BoundValue>,
    },
    /// A `NULL` list element is `None`.
    List(Vec<Option<Value>>),
    Hash {
        modulus: i64,
        remainder: i64,
    },
    Default,
}

impl PartitionKey {
    /// The key a `PARTITION BY` clause names, as stored.
    pub(crate) fn parse(text: &str) -> Result<PartitionKey> {
        let (strategy, rest) = text
            .split_once('(')
            .map(|(head, rest)| (head.trim(), rest))
            .unwrap_or((text.trim(), ""));
        let strategy = match strategy.to_ascii_uppercase().as_str() {
            "RANGE" => Strategy::Range,
            "LIST" => Strategy::List,
            "HASH" => Strategy::Hash,
            other => anyhow::bail!("unsupported partition strategy: {other}"),
        };
        let columns: Vec<String> = rest
            .trim_end_matches(')')
            .split(',')
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty())
            .collect();
        Ok(PartitionKey { strategy, columns })
    }

    /// The canonical text, as PostgreSQL renders it.
    pub(crate) fn render(&self) -> String {
        format!("{} ({})", self.strategy.name(), self.columns.join(", "))
    }
}

/// A bound's canonical text as PostgreSQL renders it.
pub(crate) fn render_bound(bound: &Bound) -> String {
    let value = |v: &BoundValue| match v {
        BoundValue::Min => "MINVALUE".to_string(),
        BoundValue::Max => "MAXVALUE".to_string(),
        BoundValue::Value(v) => render_literal(v),
    };
    let list = |values: &[BoundValue]| values.iter().map(value).collect::<Vec<_>>().join(", ");
    match bound {
        Bound::Range { from, to } => {
            format!("FOR VALUES FROM ({}) TO ({})", list(from), list(to))
        }
        Bound::List(values) => format!(
            "FOR VALUES IN ({})",
            values
                .iter()
                .map(|v| match v {
                    Some(v) => render_literal(v),
                    None => "NULL".to_string(),
                })
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Bound::Hash { modulus, remainder } => {
            format!("FOR VALUES WITH (modulus {modulus}, remainder {remainder})")
        }
        Bound::Default => "DEFAULT".to_string(),
    }
}

/// A bound value as PostgreSQL's const deparse writes it: numbers bare,
/// everything else quoted.
fn render_literal(value: &Value) -> String {
    match value {
        Value::Int(_) | Value::Float(_) | Value::Numeric(_) => crate::render(value),
        other => format!("'{}'", crate::render(other).replace('\'', "''")),
    }
}

/// Parses a bound's canonical text against the key columns' types.
pub(crate) fn parse_bound(text: &str, key: &PartitionKey, types: &[String]) -> Result<Bound> {
    let text = text.trim();
    if text.eq_ignore_ascii_case("DEFAULT") {
        return Ok(Bound::Default);
    }
    if let Some(rest) = strip_keywords(text, &["FOR", "VALUES", "IN"]) {
        if key.strategy != Strategy::List {
            anyhow::bail!("invalid bound specification for a {} partition", kind(key));
        }
        let values = split_values(rest)?;
        let values = values
            .iter()
            .map(|v| {
                if v.eq_ignore_ascii_case("NULL") {
                    Ok(None)
                } else {
                    Ok(Some(parse_literal(v, &types[0])?))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(Bound::List(values));
    }
    if let Some(rest) = strip_keywords(text, &["FOR", "VALUES", "WITH"]) {
        if key.strategy != Strategy::Hash {
            anyhow::bail!("invalid bound specification for a {} partition", kind(key));
        }
        let mut modulus = None;
        let mut remainder = None;
        for part in split_values(rest)? {
            let (name, value) = part
                .split_once(char::is_whitespace)
                .map(|(n, v)| (n.to_ascii_lowercase(), v.trim().to_string()))
                .unwrap_or((part.to_ascii_lowercase(), String::new()));
            match name.as_str() {
                "modulus" => modulus = value.parse::<i64>().ok(),
                "remainder" => remainder = value.parse::<i64>().ok(),
                other => anyhow::bail!("unrecognized hash partition bound specification: {other}"),
            }
        }
        let (Some(modulus), Some(remainder)) = (modulus, remainder) else {
            anyhow::bail!("missing modulus or remainder in hash partition bound");
        };
        return Ok(Bound::Hash { modulus, remainder });
    }
    // `FOR VALUES FROM (…) TO (…)`.
    let Some(rest) = strip_keywords(text, &["FOR", "VALUES"]) else {
        anyhow::bail!("invalid bound specification: {text}");
    };
    if key.strategy != Strategy::Range {
        anyhow::bail!("invalid bound specification for a {} partition", kind(key));
    }
    let (Some(from_at), Some(to_at)) = (find_keyword(rest, "FROM"), find_keyword(rest, "TO"))
    else {
        anyhow::bail!("invalid bound specification: {text}");
    };
    let from = parse_range_values(&rest[from_at..to_at], types)?;
    let to = parse_range_values(&rest[to_at..], types)?;
    Ok(Bound::Range { from, to })
}

fn kind(key: &PartitionKey) -> String {
    key.strategy.name().to_ascii_lowercase()
}

/// The text after `keywords`, when it starts with them.
fn strip_keywords<'a>(text: &'a str, keywords: &[&str]) -> Option<&'a str> {
    let mut rest = text;
    for keyword in keywords {
        let trimmed = rest.trim_start();
        let head = trimmed.split_whitespace().next()?;
        if !head.eq_ignore_ascii_case(keyword) && !head.eq_ignore_ascii_case(&format!("{keyword}("))
        {
            return None;
        }
        rest = &trimmed[head.len()..];
    }
    Some(rest.trim_start())
}

/// The position of `keyword` at the start of a word in `text`.
fn find_keyword(text: &str, keyword: &str) -> Option<usize> {
    let upper = text.to_ascii_uppercase();
    let keyword = keyword.to_ascii_uppercase();
    let mut at = 0;
    while let Some(found) = upper[at..].find(&keyword) {
        let position = at + found;
        let before_ok = text[..position]
            .chars()
            .next_back()
            .is_none_or(|c| !c.is_alphanumeric() && c != '_');
        let after = position + keyword.len();
        let after_ok = text[after..]
            .chars()
            .next()
            .is_none_or(|c| !c.is_alphanumeric() && c != '_');
        if before_ok && after_ok {
            return Some(position);
        }
        at = position + 1;
    }
    None
}

/// Splits a parenthesized value list, respecting quoted strings.
fn split_values(text: &str) -> Result<Vec<String>> {
    let text = text.trim();
    let inner = text
        .strip_prefix('(')
        .and_then(|t| t.strip_suffix(')'))
        .unwrap_or(text);
    let mut values = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                current.push(c);
                if quoted && chars.peek() == Some(&'\'') {
                    current.push(chars.next().expect("peeked"));
                } else {
                    quoted = !quoted;
                }
            }
            ',' if !quoted => {
                values.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(c),
        }
    }
    if !current.trim().is_empty() || !values.is_empty() {
        values.push(current.trim().to_string());
    }
    Ok(values)
}

/// The range bound's values, from `FROM (…)` or `TO (…)` onward.
fn parse_range_values(text: &str, types: &[String]) -> Result<Vec<BoundValue>> {
    let Some(start) = text.find('(') else {
        anyhow::bail!("invalid bound specification: {text}");
    };
    let Some(end) = text[start..].find(')') else {
        anyhow::bail!("invalid bound specification: {text}");
    };
    let values = split_values(&text[start..start + end + 1])?;
    let mut out = Vec::new();
    for (at, value) in values.iter().enumerate() {
        let data_type = types.get(at).map(String::as_str).unwrap_or("text");
        out.push(if value.eq_ignore_ascii_case("MINVALUE") {
            BoundValue::Min
        } else if value.eq_ignore_ascii_case("MAXVALUE") {
            BoundValue::Max
        } else if value.eq_ignore_ascii_case("NULL") {
            anyhow::bail!("range partition bound may not be NULL");
        } else {
            BoundValue::Value(parse_literal(value, data_type)?)
        });
    }
    Ok(out)
}

/// A bound literal as its key column's type reads it.
fn parse_literal(text: &str, data_type: &str) -> Result<Value> {
    let text = text.trim();
    let unquoted = text
        .strip_prefix('\'')
        .and_then(|t| t.strip_suffix('\''))
        .map(|t| t.replace("''", "'"));
    let literal = unquoted.as_deref().unwrap_or(text);
    let type_name = crate::value::sql_type_name(&data_type.to_ascii_lowercase());
    let invalid = || {
        anyhow::anyhow!(
            crate::error_fields::DbError::new(format!(
                "invalid input syntax for type {type_name}: \"{literal}\""
            ))
            .code("22P02")
            .into_text()
        )
    };
    Ok(match crate::value::column_type(data_type) {
        crate::value::ColumnType::Int => literal
            .parse::<i64>()
            .map(Value::Int)
            .map_err(|_| invalid())?,
        crate::value::ColumnType::Float => literal
            .parse::<f64>()
            .map(Value::Float)
            .map_err(|_| invalid())?,
        crate::value::ColumnType::Bool => match literal {
            "t" | "true" => Value::Bool(true),
            "f" | "false" => Value::Bool(false),
            _ => return Err(invalid()),
        },
        _ => Value::Text(literal.to_string()),
    })
}

/// Whether a row's key values fall in `bound`.
pub(crate) fn contains(bound: &Bound, values: &[Value], types: &[String]) -> bool {
    match bound {
        Bound::Default => false,
        Bound::List(elements) => {
            let Some(key) = values.first() else {
                return false;
            };
            elements.iter().any(|element| match element {
                Some(element) => crate::value::compare(key, element) == std::cmp::Ordering::Equal,
                None => *key == Value::Null,
            })
        }
        Bound::Hash { modulus, remainder } => match row_hash(values, types) {
            Some(hash) => ((hash % (*modulus as u64)) as i64) == *remainder,
            None => false,
        },
        Bound::Range { from, to } => {
            // NULL never falls in a range.
            if values.is_empty() || values.iter().any(|v| *v == Value::Null) {
                return false;
            }
            // The row must be at least `from` and strictly below `to`, per
            // the tuple's lexicographic order.
            !tuple_less(values, from) && tuple_less(values, to)
        }
    }
}

/// The key values of a row, by the parent's key columns.
pub(crate) fn key_values(
    parent: &TableDescriptor,
    key: &PartitionKey,
    row: &[Value],
) -> Result<Vec<Value>> {
    key.columns
        .iter()
        .map(|column| {
            parent
                .columns
                .iter()
                .position(|c| &c.name == column)
                .and_then(|at| row.get(at))
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("column \"{column}\" does not exist"))
        })
        .collect()
}

/// The key columns' declared types.
pub(crate) fn key_types(parent: &TableDescriptor, key: &PartitionKey) -> Vec<String> {
    key.columns
        .iter()
        .map(|column| {
            parent
                .columns
                .iter()
                .find(|c| &c.name == column)
                .map(|c| c.data_type.clone())
                .unwrap_or_else(|| "text".to_string())
        })
        .collect()
}

/// `no partition of relation "pt" found for row`, with PostgreSQL's DETAIL.
pub(crate) fn no_partition_error(
    parent: &TableDescriptor,
    key: &PartitionKey,
    values: &[Value],
) -> String {
    let columns = key.columns.join(", ");
    let rendered: Vec<String> = values
        .iter()
        .map(|v| match v {
            Value::Null => "null".to_string(),
            other => crate::render(other),
        })
        .collect();
    let detail = if key.columns.len() == 1 {
        format!(
            "Partition key of the failing row contains ({columns}) = ({}).",
            rendered[0]
        )
    } else {
        format!(
            "Partition key of the failing row contains ({columns}) = ({}).",
            rendered.join(", ")
        )
    };
    crate::error_fields::DbError::new(format!(
        "no partition of relation \"{}\" found for row",
        parent.name
    ))
    .detail(detail)
    .code("23514")
    .into_text()
}

/// `new row for relation "x" violates partition constraint`, with DETAIL.
pub(crate) fn constraint_error(table: &TableDescriptor, row: &[Value]) -> String {
    let rendered: Vec<String> = row.iter().map(crate::render).collect();
    crate::error_fields::DbError::new(format!(
        "new row for relation \"{}\" violates partition constraint",
        table.name
    ))
    .detail(format!("Failing row contains ({}).", rendered.join(", ")))
    .code("23514")
    .into_text()
}

/// `HASH_PARTITION_SEED` from PostgreSQL's `catalog/partition.h`: hash
/// partitioning hashes every key value with this seed.
const HASH_PARTITION_SEED: u64 = 0x7A5B_2236_7996_DCFD;

/// PostgreSQL's `hash_combine64`, which folds a key column's hash into the
/// row's.
fn hash_combine64(a: u64, b: u64) -> u64 {
    a ^ b
        .wrapping_add(0x49a0_f4dd_15e5_a8e3)
        .wrapping_add(a << 54)
        .wrapping_add(a >> 7)
}

/// PostgreSQL's lookup3 `hash_bytes` (and its seeded 64-bit variant), over
/// little-endian bytes. A seed of zero takes the unseeded path.
pub(crate) fn hash_lookup3(bytes: &[u8], seed: u64) -> (u32, u32, u32) {
    fn rot(x: u32, k: u32) -> u32 {
        x.rotate_left(k)
    }
    macro_rules! mix {
        ($a:ident, $b:ident, $c:ident) => {
            $a = $a.wrapping_sub($c);
            $a ^= rot($c, 4);
            $c = $c.wrapping_add($b);
            $b = $b.wrapping_sub($a);
            $b ^= rot($a, 6);
            $a = $a.wrapping_add($c);
            $c = $c.wrapping_sub($b);
            $c ^= rot($b, 8);
            $b = $b.wrapping_add($a);
            $a = $a.wrapping_sub($c);
            $a ^= rot($c, 16);
            $c = $c.wrapping_add($b);
            $b = $b.wrapping_sub($a);
            $b ^= rot($a, 19);
            $a = $a.wrapping_add($c);
            $c = $c.wrapping_sub($b);
            $c ^= rot($b, 4);
            $b = $b.wrapping_add($a);
        };
    }
    let mut len = bytes.len();
    let mut a = 0x9e37_79b9u32
        .wrapping_add(len as u32)
        .wrapping_add(3_923_095);
    let mut b = a;
    let mut c = a;
    // A non-zero seed perturbs the state as if it were a 12-byte chunk.
    if seed != 0 {
        a = a.wrapping_add((seed >> 32) as u32);
        b = b.wrapping_add(seed as u32);
        mix!(a, b, c);
    }
    let mut at = 0;
    while len >= 12 {
        a = a.wrapping_add(u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()));
        b = b.wrapping_add(u32::from_le_bytes(
            bytes[at + 4..at + 8].try_into().unwrap(),
        ));
        c = c.wrapping_add(u32::from_le_bytes(
            bytes[at + 8..at + 12].try_into().unwrap(),
        ));
        mix!(a, b, c);
        at += 12;
        len -= 12;
    }
    let tail = &bytes[at..];
    let word = |i: usize| -> u32 {
        (0..4)
            .map(|k| {
                if k < len {
                    (tail[i + k] as u32) << (8 * k)
                } else {
                    0
                }
            })
            .sum()
    };
    match len {
        11 => {
            c = c.wrapping_add((tail[10] as u32) << 24);
            c = c.wrapping_add((tail[9] as u32) << 16);
            c = c.wrapping_add((tail[8] as u32) << 8);
            b = b.wrapping_add(word(4));
            a = a.wrapping_add(word(0));
        }
        10 => {
            c = c.wrapping_add((tail[9] as u32) << 16);
            c = c.wrapping_add((tail[8] as u32) << 8);
            b = b.wrapping_add(word(4));
            a = a.wrapping_add(word(0));
        }
        9 => {
            c = c.wrapping_add((tail[8] as u32) << 8);
            b = b.wrapping_add(word(4));
            a = a.wrapping_add(word(0));
        }
        8 => {
            b = b.wrapping_add(word(4));
            a = a.wrapping_add(word(0));
        }
        7 => {
            b = b.wrapping_add((tail[6] as u32) << 16);
            b = b.wrapping_add((tail[5] as u32) << 8);
            b = b.wrapping_add(tail[4] as u32);
            a = a.wrapping_add(word(0));
        }
        6 => {
            b = b.wrapping_add((tail[5] as u32) << 8);
            b = b.wrapping_add(tail[4] as u32);
            a = a.wrapping_add(word(0));
        }
        5 => {
            b = b.wrapping_add(tail[4] as u32);
            a = a.wrapping_add(word(0));
        }
        4 => a = a.wrapping_add(word(0)),
        3 => {
            a = a.wrapping_add((tail[2] as u32) << 16);
            a = a.wrapping_add((tail[1] as u32) << 8);
            a = a.wrapping_add(tail[0] as u32);
        }
        2 => {
            a = a.wrapping_add((tail[1] as u32) << 8);
            a = a.wrapping_add(tail[0] as u32);
        }
        1 => a = a.wrapping_add(tail[0] as u32),
        _ => {}
    }
    c ^= b;
    c = c.wrapping_sub(rot(b, 14));
    a ^= c;
    a = a.wrapping_sub(rot(c, 11));
    b ^= a;
    b = b.wrapping_sub(rot(a, 25));
    c ^= b;
    c = c.wrapping_sub(rot(b, 16));
    a ^= c;
    a = a.wrapping_sub(rot(c, 4));
    b ^= a;
    b = b.wrapping_sub(rot(a, 14));
    c ^= b;
    c = c.wrapping_sub(rot(b, 24));
    (a, b, c)
}

/// The OID of the default operator class a key column of `data_type` gets
/// under `strategy`, as `pg_partitioned_table.partclass` shows it.
pub(crate) fn opclass_oid(data_type: &str, strategy: Strategy) -> Option<i64> {
    let name = crate::value::sql_type_name(&data_type.to_ascii_lowercase());
    let hash = strategy == Strategy::Hash;
    Some(match (name.as_str(), hash) {
        ("smallint", false) => 1979,
        ("smallint", true) => 10019,
        ("integer", false) => 1978,
        ("integer", true) => 10020,
        ("bigint", false) => 3124,
        ("bigint", true) => 10021,
        ("text" | "character varying", false) => 3126,
        ("text" | "character varying", true) => 10037,
        ("character", false) => 10004,
        ("character", true) => 10005,
        ("name", false) => 10028,
        ("name", true) => 10029,
        ("numeric", false) => 3125,
        ("numeric", true) => 10030,
        ("boolean", false) => 10003,
        ("boolean", true) => 10048,
        ("date", false) => 3122,
        ("date", true) => 10011,
        ("real", false) => 10012,
        ("real", true) => 10013,
        ("double precision", false) => 3123,
        ("double precision", true) => 10014,
        ("timestamp without time zone", false) => 3128,
        ("timestamp without time zone", true) => 10046,
        ("timestamp with time zone", false) => 3127,
        ("timestamp with time zone", true) => 10040,
        ("time without time zone", false) => 10038,
        ("time without time zone", true) => 10039,
        ("interval", false) => 10022,
        ("interval", true) => 10023,
        ("uuid", false) => 10065,
        ("uuid", true) => 10066,
        ("bytea", false) => 10006,
        ("bytea", true) => 10049,
        _ => return None,
    })
}

/// Whether `data_type` has a collation (as `pg_partitioned_table` shows for
/// the key columns).
pub(crate) fn is_collatable(data_type: &str) -> bool {
    matches!(
        crate::value::sql_type_name(&data_type.to_ascii_lowercase()).as_str(),
        "text" | "character varying" | "character" | "name"
    )
}

/// `hash_bytes_uint32_extended`: the seeded hash of the little-endian word.
fn hash_uint32_extended(value: u32, seed: u64) -> u64 {
    let (_, b, c) = hash_lookup3(&value.to_le_bytes(), seed);
    ((b as u64) << 32) | c as u64
}

/// A key value's hash as PostgreSQL's hash partitioning computes it: the
/// type's *extended* hash support function, seeded with
/// [`HASH_PARTITION_SEED`].
pub(crate) fn hash_value(value: &Value, data_type: &str) -> Option<u64> {
    let type_name = crate::value::sql_type_name(&data_type.to_ascii_lowercase());
    Some(match (type_name.as_str(), value) {
        ("smallint", Value::Int(v)) => hash_uint32_extended(*v as i32 as u32, HASH_PARTITION_SEED),
        ("integer", Value::Int(v)) => hash_uint32_extended(*v as i32 as u32, HASH_PARTITION_SEED),
        ("bigint", Value::Int(v)) => {
            let lohalf = *v as u32;
            let hihalf = (*v >> 32) as u32;
            let lohalf = lohalf ^ if *v >= 0 { hihalf } else { !hihalf };
            hash_uint32_extended(lohalf, HASH_PARTITION_SEED)
        }
        ("text" | "character varying" | "character" | "name", Value::Text(s)) => {
            let (_, b, c) = hash_lookup3(s.as_bytes(), HASH_PARTITION_SEED);
            ((b as u64) << 32) | c as u64
        }
        _ => return None,
    })
}

/// The row hash: each non-NULL key column's hash combined in key order, as
/// PostgreSQL's `compute_partition_hash_value` does. A column type without
/// a hash support function hashes nothing (no partition claims it).
pub(crate) fn row_hash(values: &[Value], types: &[String]) -> Option<u64> {
    let mut combined = 0u64;
    for (at, value) in values.iter().enumerate() {
        // NULLs are skipped.
        if *value == Value::Null {
            continue;
        }
        let data_type = types.get(at).map(String::as_str).unwrap_or("text");
        combined = hash_combine64(combined, hash_value(value, data_type)?);
    }
    Some(combined)
}

/// Whether the key `values` sort before the bound tuple `bound`
/// lexicographically, `MINVALUE` below and `MAXVALUE` above everything.
fn tuple_less(values: &[Value], bound: &[BoundValue]) -> bool {
    use std::cmp::Ordering;
    for at in 0..bound.len() {
        match &bound[at] {
            // Below everything, and everything before this element agreed.
            BoundValue::Min => return false,
            // Above everything: whatever is left is below it.
            BoundValue::Max => return true,
            BoundValue::Value(high) => {
                let Some(value) = values.get(at) else {
                    return true;
                };
                match crate::value::compare(value, high) {
                    Ordering::Less => return true,
                    Ordering::Greater => return false,
                    Ordering::Equal => {}
                }
            }
        }
    }
    false
}

/// Whether two siblings' bounds claim a common key. Ranges are
/// half-open intervals, lists share an element (NULL matching NULL), and
/// hash remainders collide when the congruences have a common solution.
pub(crate) fn bounds_overlap(a: &Bound, b: &Bound, types: &[String]) -> bool {
    match (a, b) {
        (
            Bound::Hash {
                modulus: m1,
                remainder: r1,
            },
            Bound::Hash {
                modulus: m2,
                remainder: r2,
            },
        ) => {
            let gcd = gcd(*m1, *m2);
            r1.rem_euclid(gcd) == r2.rem_euclid(gcd)
        }
        (Bound::Range { from: f1, to: t1 }, Bound::Range { from: f2, to: t2 }) => {
            bound_vec_cmp(f1, t2) == std::cmp::Ordering::Less
                && bound_vec_cmp(f2, t1) == std::cmp::Ordering::Less
        }
        (Bound::List(l1), Bound::List(l2)) => l1.iter().any(|a| {
            l2.iter().any(|b| match (a, b) {
                (None, None) => true,
                (Some(a), Some(b)) => crate::value::compare(a, b) == std::cmp::Ordering::Equal,
                _ => false,
            })
        }),
        _ => false,
    }
}

/// Lexicographic comparison of range bound values, `MINVALUE` below and
/// `MAXVALUE` above everything.
fn bound_vec_cmp(a: &[BoundValue], b: &[BoundValue]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for at in 0..a.len().max(b.len()) {
        let ord = match (a.get(at), b.get(at)) {
            (Some(BoundValue::Min), Some(BoundValue::Min)) | (None, None) => Ordering::Equal,
            (Some(BoundValue::Min), _) | (_, None) => Ordering::Less,
            (Some(BoundValue::Max), Some(BoundValue::Max)) => Ordering::Equal,
            (Some(BoundValue::Max), _) => Ordering::Greater,
            (_, Some(BoundValue::Min)) => Ordering::Greater,
            (_, Some(BoundValue::Max)) => Ordering::Less,
            (Some(BoundValue::Value(x)), Some(BoundValue::Value(y))) => crate::value::compare(x, y),
            (None, Some(_)) => Ordering::Less,
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

fn gcd(a: i64, b: i64) -> i64 {
    if b == 0 { a } else { gcd(b, a % b) }
}

impl crate::MemExecutor {
    /// A partitioned table's direct partitions, siblings in creation order.
    pub(crate) fn partition_children(
        &self,
        db_name: &str,
        id: nodus_catalog::TableId,
    ) -> Result<Vec<TableDescriptor>> {
        let mut children: Vec<TableDescriptor> = self
            .catalog_reader
            .list_all_tables(db_name)?
            .into_iter()
            .filter(|t| t.parents.contains(&id))
            .collect();
        children
            .sort_by(|a, b| (a.created_at, a.name.as_str()).cmp(&(b.created_at, b.name.as_str())));
        Ok(children)
    }

    /// The partition of a partitioned table that holds `row`, descending
    /// through sub-partitions; the table itself when it is not partitioned.
    /// A key no partition claims is PostgreSQL's `no partition of relation
    /// "pt" found for row`.
    pub(crate) fn route_row(
        &self,
        db_name: &str,
        table: &TableDescriptor,
        row: &[Value],
    ) -> Result<TableDescriptor> {
        let mut current = table.clone();
        while let Some(text) = &current.partition_by {
            let key = PartitionKey::parse(text)?;
            let values = key_values(&current, &key, row)?;
            let types = key_types(&current, &key);
            let children = self.partition_children(db_name, current.id)?;
            let found = children
                .iter()
                .find(|child| {
                    child
                        .partition_bound
                        .as_deref()
                        .and_then(|text| parse_bound(text, &key, &types).ok())
                        .is_some_and(|bound| contains(&bound, &values, &types))
                })
                // A row no bound claims falls to the default partition.
                .or_else(|| {
                    children
                        .iter()
                        .find(|child| child.partition_bound.as_deref() == Some("DEFAULT"))
                });
            match found {
                Some(child) => current = child.clone(),
                None => anyhow::bail!(no_partition_error(&current, &key, &values)),
            }
        }
        Ok(current)
    }

    /// The checks PostgreSQL makes when a partition's bound is fixed: no
    /// sibling may overlap it, only one default may exist, and a default
    /// partition's rows must still belong to it. `candidate` names the
    /// partition being created or attached; `exclude` is its id, when it
    /// already exists.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn check_partition_bound(
        &self,
        ctx: &crate::ExecutionContext,
        parent: &TableDescriptor,
        key: &PartitionKey,
        types: &[String],
        bound: &Bound,
        candidate: &str,
        exclude: Option<nodus_catalog::TableId>,
    ) -> Result<()> {
        let mut default: Option<TableDescriptor> = None;
        for sibling in self
            .partition_children("default", parent.id)?
            .iter()
            .filter(|s| Some(s.id) != exclude)
        {
            let Some(text) = sibling.partition_bound.as_deref() else {
                continue;
            };
            let sibling_bound = parse_bound(text, key, types)?;
            match (&sibling_bound, bound) {
                (Bound::Default, Bound::Default) => anyhow::bail!(
                    "partition \"{candidate}\" conflicts with existing default partition \"{}\"",
                    sibling.name
                ),
                (Bound::Default, _) => default = Some(sibling.clone()),
                (_, Bound::Default) => {}
                (
                    Bound::Hash {
                        modulus: other_modulus,
                        ..
                    },
                    Bound::Hash { modulus, .. },
                ) if modulus != other_modulus => {
                    if modulus % other_modulus != 0 {
                        anyhow::bail!(crate::error_fields::DbError::new(
                            "every hash partition modulus must be a factor of the next larger modulus"
                        )
                        .detail(format!(
                            "The new modulus {modulus} is not divisible by {other_modulus}, the modulus of existing partition \"{}\".",
                            sibling.name
                        ))
                        .into_text());
                    }
                    if !bounds_overlap(&sibling_bound, bound, types) {
                        continue;
                    }
                    anyhow::bail!(
                        "partition \"{candidate}\" would overlap partition \"{}\"",
                        sibling.name
                    )
                }
                _ => {
                    if bounds_overlap(&sibling_bound, bound, types) {
                        anyhow::bail!(
                            "partition \"{candidate}\" would overlap partition \"{}\"",
                            sibling.name
                        )
                    }
                }
            }
        }
        if let Some(def) = default {
            // Rows the default holds must not fall into the new bound.
            for (_key, row) in self.scan_rows_keyed(def.id, &ctx.session_id)? {
                let values = key_values(parent, key, &row)?;
                if contains(bound, &values, types) {
                    anyhow::bail!(
                        "updated partition constraint for default partition \"{}\" would be violated by some row",
                        def.name
                    );
                }
            }
        }
        Ok(())
    }

    /// Enforces the partition constraint of every partitioned ancestor on a
    /// row written straight into `table` (its own bound for a leaf, the
    /// siblings' for a default partition).
    pub(crate) fn check_partition_constraint(
        &self,
        db_name: &str,
        table: &TableDescriptor,
        row: &[Value],
    ) -> Result<()> {
        let mut current = table.clone();
        while let Some(parent) = current
            .parents
            .first()
            .and_then(|id| self.catalog_reader.get_table_by_id(*id).ok())
        {
            if let Some(text) = &parent.partition_by {
                let key = PartitionKey::parse(text)?;
                let values = key_values(&parent, &key, row)?;
                let types = key_types(&parent, &key);
                let bound = current
                    .partition_bound
                    .as_deref()
                    .map(|text| parse_bound(text, &key, &types))
                    .transpose()?;
                let ok = match &bound {
                    // A default partition holds what no sibling claims.
                    Some(Bound::Default) => !self
                        .partition_children(db_name, parent.id)?
                        .iter()
                        .filter(|sibling| sibling.id != current.id)
                        .filter_map(|sibling| {
                            sibling
                                .partition_bound
                                .as_deref()
                                .and_then(|text| parse_bound(text, &key, &types).ok())
                        })
                        .any(|bound| contains(&bound, &values, &types)),
                    Some(bound) => contains(bound, &values, &types),
                    None => false,
                };
                if !ok {
                    anyhow::bail!(constraint_error(table, row));
                }
            }
            current = parent;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dml_join_tests::{rows, session};

    /// The rows of a select, rendered.
    fn values(
        sql: &impl Fn(&str) -> anyhow::Result<crate::QueryOutput>,
        statement: &str,
    ) -> Vec<String> {
        rows(&sql(statement).unwrap())
    }

    #[test]
    fn partitions_route_inserts_and_updates() {
        let sql = session();
        sql("create table pt (id int, v text) partition by range (id)").unwrap();
        // No partition yet: the row has nowhere to go.
        assert!(sql("insert into pt values (5, 'a')").is_err());
        sql("create table pt1 partition of pt for values from (1) to (10)").unwrap();
        sql("create table ptd partition of pt default").unwrap();
        sql("insert into pt values (5, 'a'), (50, 'b')").unwrap();
        assert_eq!(
            values(&sql, "select tableoid::regclass, id from pt order by id"),
            ["pt1|5", "ptd|50"]
        );
        // A leaf takes no row outside its bound; the default takes no row a
        // sibling claims.
        assert!(sql("insert into pt1 values (60, 'x')").is_err());
        assert!(sql("insert into ptd values (6, 'x')").is_err());
        // A key change moves the row through the parent; through a
        // partition it must stay inside.
        sql("update pt set id = 7 where id = 5").unwrap();
        assert!(sql("update pt1 set id = 60 where id = 7").is_err());
        sql("update pt set id = 60 where id = 7").unwrap();
        assert_eq!(
            values(&sql, "select tableoid::regclass, id from pt order by id"),
            ["ptd|50", "ptd|60"]
        );
        // The parent holds no rows; the catalogs show the shape.
        assert_eq!(values(&sql, "select count(*) from only pt"), ["0"]);
        assert_eq!(
            values(
                &sql,
                "select relkind, relispartition from pg_class where relname = 'pt1'"
            ),
            ["r|t"]
        );
        assert_eq!(
            values(&sql, "select pg_get_partkeydef('pt'::regclass)"),
            ["RANGE (id)"]
        );
    }

    #[test]
    fn hash_partitions_place_rows_as_postgresql_does() {
        let sql = session();
        sql("create table h2 (id int) partition by hash (id)").unwrap();
        for remainder in 0..4 {
            sql(&format!(
                "create table h2r{remainder} partition of h2 for values with (modulus 4, remainder {remainder})"
            ))
            .unwrap();
        }
        sql("insert into h2 select generate_series(1, 10)").unwrap();
        // PostgreSQL 18.4's own placement: remainder 1 holds 3, 5, 8, 9.
        assert_eq!(
            values(
                &sql,
                "select string_agg(id::text, ',' order by id) from h2r1"
            ),
            ["3,5,8,9"]
        );
        assert_eq!(
            values(
                &sql,
                "select string_agg(id::text, ',' order by id) from h2r3"
            ),
            ["4,6,7,10"]
        );
        // The same remainder twice would overlap.
        assert!(
            sql("create table h2x partition of h2 for values with (modulus 4, remainder 0)")
                .is_err()
        );
    }

    #[test]
    fn attach_and_detach_change_the_link() {
        let sql = session();
        sql("create table at (id int) partition by range (id)").unwrap();
        sql("create table at1 (id int)").unwrap();
        sql("insert into at1 values (5)").unwrap();
        // The rows must fall inside the bound being attached.
        assert!(sql("alter table at attach partition at1 for values from (1) to (3)").is_err());
        sql("alter table at attach partition at1 for values from (1) to (10)").unwrap();
        assert_eq!(values(&sql, "select id from at"), ["5"]);
        assert_eq!(
            values(
                &sql,
                "select relispartition from pg_class where relname = 'at1'"
            ),
            ["t"]
        );
        sql("alter table at detach partition at1").unwrap();
        // The table keeps its rows and stops being a partition.
        assert_eq!(values(&sql, "select id from at1"), ["5"]);
        assert!(sql("insert into at values (5)").is_err());
    }

    #[test]
    fn keys_and_bounds_parse_and_render() {
        let key = PartitionKey::parse("RANGE (id, v)").unwrap();
        assert_eq!(key.strategy, Strategy::Range);
        assert_eq!(key.columns, ["id", "v"]);
        assert_eq!(key.render(), "RANGE (id, v)");
        let types = ["integer".to_string(), "text".to_string()];
        let bound = parse_bound("FOR VALUES FROM (1, 'a') TO (3, 'b')", &key, &types).unwrap();
        assert_eq!(render_bound(&bound), "FOR VALUES FROM (1, 'a') TO (3, 'b')");
        let bound = parse_bound("FOR VALUES FROM (MINVALUE) TO (MAXVALUE)", &key, &types).unwrap();
        assert_eq!(
            render_bound(&bound),
            "FOR VALUES FROM (MINVALUE) TO (MAXVALUE)"
        );
        let list = PartitionKey::parse("LIST (v)").unwrap();
        let bound =
            parse_bound("FOR VALUES IN ('a,b', NULL)", &list, &["text".to_string()]).unwrap();
        assert_eq!(render_bound(&bound), "FOR VALUES IN ('a,b', NULL)");
        let hash = PartitionKey::parse("HASH (id)").unwrap();
        let bound = parse_bound(
            "FOR VALUES WITH (modulus 4, remainder 1)",
            &hash,
            &["integer".to_string()],
        )
        .unwrap();
        assert_eq!(
            render_bound(&bound),
            "FOR VALUES WITH (modulus 4, remainder 1)"
        );
        assert_eq!(render_bound(&Bound::Default), "DEFAULT");
        assert!(parse_bound("FOR VALUES IN (1)", &key, &types).is_err());
        assert!(parse_bound("FOR VALUES FROM ('x') TO ('y')", &key, &types).is_err());
    }

    #[test]
    fn bounds_match_their_values() {
        let types = ["integer".to_string()];
        let key = PartitionKey::parse("RANGE (id)").unwrap();
        let bound = parse_bound("FOR VALUES FROM (1) TO (10)", &key, &types).unwrap();
        assert!(contains(&bound, &[Value::Int(1)], &types));
        assert!(contains(&bound, &[Value::Int(9)], &types));
        assert!(!contains(&bound, &[Value::Int(10)], &types));
        assert!(!contains(&bound, &[Value::Int(0)], &types));
        assert!(!contains(&bound, &[Value::Null], &types));
        let list = PartitionKey::parse("LIST (v)").unwrap();
        let ltypes = ["text".to_string()];
        let bound = parse_bound("FOR VALUES IN ('a', NULL)", &list, &ltypes).unwrap();
        assert!(contains(&bound, &[Value::Text("a".into())], &ltypes));
        assert!(!contains(&bound, &[Value::Text("b".into())], &ltypes));
        assert!(contains(&bound, &[Value::Null], &ltypes));
    }

    #[test]
    fn hashes_match_postgresql() {
        // The distribution PostgreSQL's own hash partitioning gives, from
        // `select v, ... satisfies_hash_partition('h2'::regclass, 4, r, v)`
        // on 18.4: remainder 0: 1, 1000; 1: 3, 5, 8, 9, 100; 2: 2;
        // 3: 4, 6, 7, 10.
        let remainder = |value: i64| {
            let types = ["integer".to_string()];
            (row_hash(&[Value::Int(value)], &types).expect("integer hashes") % 4) as i64
        };
        for (value, expected) in [
            (1, 0),
            (1000, 0),
            (3, 1),
            (5, 1),
            (8, 1),
            (9, 1),
            (100, 1),
            (2, 2),
            (4, 3),
            (6, 3),
            (7, 3),
            (10, 3),
        ] {
            assert_eq!(remainder(value), expected, "remainder of {value}");
        }
    }
}
